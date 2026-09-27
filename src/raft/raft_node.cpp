#include "raft_node.h"
#include <algorithm>
#include <chrono>
#include <random>

namespace raft
{

    RaftNode::RaftNode(int node_id, const Config &config)
        : node_id_(node_id), config_(config), running_(false),
          last_heartbeat_(std::chrono::steady_clock::now()),
          election_timeout_ms_(0), gen_(rd_()),
          timeout_dist_(MIN_ELECTION_TIMEOUT_MS, MAX_ELECTION_TIMEOUT_MS)
    {
        logger_ = std::make_unique<Logger>("raft_node_" + std::to_string(node_id_));
        logger_->set_level(Logger::string_to_level(config_.get_log_level()));

        state_ = std::make_unique<RaftState>(node_id_);

        persistent_state_ = std::make_unique<PersistentState>(config_.get_data_directory());
        if (!persistent_state_->initialize())
        {
            throw std::runtime_error("Failed to initialize persistent state");
        }

        log_storage_ = std::make_unique<LogStorage>(config_.get_data_directory());
        if (!log_storage_->initialize())
        {
            throw std::runtime_error("Failed to initialize log storage");
        }

        network_manager_ = std::make_unique<NetworkManager>(
            node_id_, config_.get_listen_address(), config_.get_listen_port());

        std::vector<uint32_t> cluster_nodes;
        for (const auto &node : config_.get_cluster_nodes())
        {
            cluster_nodes.push_back(node.node_id);
            if (node.node_id != static_cast<uint32_t>(node_id_))
            {
                peer_nodes_.push_back(node.node_id);
                logger_->info("Adding peer {} at {}:{}", node.node_id, node.address, node.port);
                network_manager_->add_peer(node.node_id, node.address, node.port);
            }
        }
        state_->set_cluster_nodes(cluster_nodes);

        network_manager_->set_message_handler(
            [this](std::unique_ptr<Message> message)
            {
                handle_message(std::move(message));
            });

        network_manager_->set_connection_callback(
            [this](uint32_t node_id, bool connected)
            {
                handle_connection_change(node_id, connected);
            });

        state_->set_current_term(persistent_state_->get_current_term());
        state_->set_voted_for(persistent_state_->get_voted_for());

        reset_election_timeout();

        logger_->info("Raft node {} initialized with {} peers", node_id_, peer_nodes_.size());
    }

    RaftNode::~RaftNode()
    {
        stop();
    }

    void RaftNode::send_outbox(std::unique_lock<std::mutex> &lock)
    {
        std::vector<std::unique_ptr<Message>> messages;
        messages.swap(outbox_);
        lock.unlock();

        for (auto &message : messages)
        {
            uint32_t dest = message->dest_node_id;
            network_manager_->send_message(dest, std::move(message));
        }

        lock.lock();
    }

    void RaftNode::flush_outbox()
    {
        std::unique_lock<std::mutex> lock(state_mutex_);
        send_outbox(lock);
    }

    void RaftNode::start()
    {
        if (running_.exchange(true))
            return;

        logger_->info("Starting Raft node {}", node_id_);

        if (!network_manager_->start())
        {
            running_.store(false);
            throw std::runtime_error("Failed to start network manager");
        }

        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            reset_election_timeout();
        }

        election_timer_thread_ = std::thread(&RaftNode::election_timer_thread, this);
        heartbeat_timer_thread_ = std::thread(&RaftNode::heartbeat_timer_thread, this);

        logger_->info("Raft node {} started", node_id_);
    }

    void RaftNode::stop()
    {
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            if (!running_.load())
                return;
            running_.store(false);
        }
        state_cv_.notify_all();

        logger_->info("Stopping Raft node {}", node_id_);

        network_manager_->stop();

        if (election_timer_thread_.joinable())
            election_timer_thread_.join();
        if (heartbeat_timer_thread_.joinable())
            heartbeat_timer_thread_.join();

        persistent_state_->sync();
        log_storage_->sync();

        logger_->info("Raft node {} stopped", node_id_);
    }

    void RaftNode::start_election()
    {
        for (int peer_id : peer_nodes_)
        {
            auto request = std::make_unique<RequestVoteRPC>(node_id_, peer_id);
            request->term = state_->get_current_term();
            request->candidate_id = node_id_;
            request->last_log_index = log_storage_->get_last_index();
            request->last_log_term = log_storage_->get_last_term();
            outbox_.push_back(std::move(request));
        }

        if (state_->has_majority_votes())
            become_leader();
    }

    void RaftNode::election_timer_thread()
    {
        std::unique_lock<std::mutex> lock(state_mutex_);
        while (running_.load())
        {
            if (state_->is_leader())
            {
                state_cv_.wait_for(lock, std::chrono::milliseconds(HEARTBEAT_INTERVAL_MS));
                continue;
            }

            auto deadline = last_heartbeat_.load() + std::chrono::milliseconds(election_timeout_ms_.load());
            if (std::chrono::steady_clock::now() >= deadline)
            {
                become_candidate();
                send_outbox(lock);
                continue;
            }

            state_cv_.wait_until(lock, deadline);
        }
    }

    void RaftNode::heartbeat_timer_thread()
    {
        std::unique_lock<std::mutex> lock(state_mutex_);
        while (running_.load())
        {
            if (state_->is_leader())
            {
                send_append_entries();
                send_outbox(lock);
            }

            state_cv_.wait_for(lock, std::chrono::milliseconds(HEARTBEAT_INTERVAL_MS),
                            [this] { return !running_.load(); });
        }
    }

    bool RaftNode::is_leader() const
    {
        return state_->is_leader();
    }

    bool RaftNode::is_running() const
    {
        return running_.load();
    }

    int RaftNode::get_current_term() const
    {
        return state_->get_current_term();
    }

    int RaftNode::get_leader_id() const
    {
        return state_->get_leader_id();
    }

    NodeState RaftNode::get_state() const
    {
        return state_->get_state();
    }

    bool RaftNode::client_request(const std::string &operation, const std::string &key,
                                  const std::string &value, std::string &response)
    {
        if (!is_leader())
        {
            response = "Not leader, redirect to node " + std::to_string(get_leader_id());
            return false;
        }

        // Create KV operation
        KVOperation::Type op_type;
        if (operation == "GET")
            op_type = KVOperation::Type::GET;
        else if (operation == "PUT")
            op_type = KVOperation::Type::PUT;
        else if (operation == "DELETE")
            op_type = KVOperation::Type::DELETE;
        else
        {
            response = "Invalid operation: " + operation;
            return false;
        }

        KVOperation kv_op(op_type, key, value);

        uint32_t next_index = log_storage_->get_last_index() + 1;
        LogEntry entry = kv_op.to_log_entry(state_->get_current_term(), next_index);

        if (!log_storage_->append_entry(entry))
        {
            response = "Failed to append to log";
            return false;
        }

        logger_->debug("Appended log entry {} for {} {}", next_index, operation, key);

        // For GET operations, we can respond immediately from local state
        if (op_type == KVOperation::Type::GET)
        {
            // In a real implementation, you'd query the state machine here
            response = "GET operation processed"; // Placeholder
            return true;
        }

        // For PUT/DELETE, we need to replicate to majority
        // This is a simplified implementation - in practice you'd wait for replication
        send_append_entries();

        response = "Operation replicated";
        return true;
    }

    void RaftNode::handle_request_vote(const RequestVoteRPC &request, RequestVoteResponse &response)
    {
        std::lock_guard<std::mutex> lock(state_mutex_);

        response.term = state_->get_current_term();
        response.vote_granted = false;

        logger_->debug("Received RequestVote from {} for term {}",
                       request.candidate_id, request.term);

        if (request.term < state_->get_current_term())
        {
            logger_->debug("Rejecting vote for {} - stale term", request.candidate_id);
            return;
        }

        if (request.term > state_->get_current_term())
        {
            become_follower(request.term);
            response.term = request.term;
        }

        if (should_grant_vote(request))
        {
            response.vote_granted = true;
            state_->set_voted_for(request.candidate_id);
            persistent_state_->set_voted_for(request.candidate_id);
            reset_election_timeout();

            logger_->info("Granted vote to {} for term {}",
                          request.candidate_id, request.term);
        }
        else
        {
            logger_->debug("Denied vote to {} for term {}",
                           request.candidate_id, request.term);
        }
    }

    void RaftNode::handle_append_entries(const AppendEntriesRPC &request, AppendEntriesResponse &response)
    {
        std::lock_guard<std::mutex> lock(state_mutex_);

        response.term = state_->get_current_term();
        response.success = false;
        response.match_index = 0;
        response.conflict_index = 0;
        response.conflict_term = 0;

        if (request.term < state_->get_current_term())
            return;

        if (request.term > state_->get_current_term() || state_->is_candidate())
        {
            become_follower(request.term);
            response.term = request.term;
        }

        state_->set_leader_id(request.leader_id);
        reset_election_timeout();

        if (request.prev_log_index > 0)
        {
            if (!log_storage_->has_entry(request.prev_log_index))
            {
                response.conflict_index = log_storage_->get_last_index() + 1;
                return;
            }

            uint32_t prev_term = log_storage_->get_entry(request.prev_log_index).term;
            if (prev_term != request.prev_log_term)
            {
                uint32_t first = log_storage_->get_first_index();
                uint32_t i = request.prev_log_index;
                while (i > first && log_storage_->get_entry(i - 1).term == prev_term)
                    --i;
                response.conflict_term = prev_term;
                response.conflict_index = i;
                return;
            }
        }

        uint32_t index = request.prev_log_index;
        for (const auto &entry : request.entries)
        {
            ++index;
            if (log_storage_->has_entry(index))
            {
                if (log_storage_->get_entry(index).term == entry.term)
                    continue;
                log_storage_->truncate_from(index);
            }
            if (!log_storage_->append_entry(entry))
            {
                logger_->error("Failed to append entry {}", entry.index);
                return;
            }
        }

        uint32_t new_commit = std::min(request.leader_commit, index);
        if (new_commit > state_->get_commit_index())
            state_->set_commit_index(new_commit);

        response.success = true;
        response.match_index = index;

        apply_committed_entries();
    }

    size_t RaftNode::get_log_size() const
    {
        return log_storage_->get_entry_count();
    }

    const LogEntry &RaftNode::get_log_entry(size_t index) const
    {
        static LogEntry empty_entry;
        LogEntry entry = log_storage_->get_entry(static_cast<uint32_t>(index));
        return entry.index != 0 ? entry : empty_entry;
    }

    void RaftNode::become_follower(int term)
    {
        logger_->info("Node {} becoming FOLLOWER for term {}", node_id_, term);

        state_->set_state(NodeState::FOLLOWER);
        state_->clear_leader_id();

        if (static_cast<uint32_t>(term) > state_->get_current_term())
        {
            state_->set_current_term(term);
            state_->clear_voted_for();
            persistent_state_->set_current_term(term);
            persistent_state_->clear_voted_for();
        }

        reset_election_timeout();
    }

    void RaftNode::become_candidate()
    {
        logger_->info("Node {} becoming CANDIDATE for term {}",
                      node_id_, state_->get_current_term() + 1);

        state_->set_state(NodeState::CANDIDATE);
        state_->increment_current_term();
        state_->set_voted_for(node_id_);
        state_->clear_leader_id();

        persistent_state_->set_current_term(state_->get_current_term());
        persistent_state_->set_voted_for(node_id_);

        // Reset election state
        state_->reset_election_state();

        reset_election_timeout();
        start_election();
    }

    void RaftNode::become_leader()
    {
        logger_->info("Node {} becoming LEADER for term {}", node_id_, state_->get_current_term());

        state_->set_state(NodeState::LEADER);
        state_->set_leader_id(node_id_);

        uint32_t last_index = log_storage_->get_last_index();
        state_->initialize_leader_state(last_index);
        log_storage_->append_entry(log_utils::create_no_op_entry(state_->get_current_term(), last_index + 1));

        send_append_entries();
    }

    void RaftNode::send_append_entries()
    {
        if (!is_leader())
            return;

        for (int peer_id : peer_nodes_)
        {
            send_append_entries_to_node(peer_id);
        }
    }

    void RaftNode::send_append_entries_to_node(int node_id)
    {
        uint32_t next_index = state_->get_next_index(node_id);
        uint32_t prev_log_index = next_index - 1;
        uint32_t prev_log_term = 0;

        if (prev_log_index > 0)
        {
            LogEntry prev_entry = log_storage_->get_entry(prev_log_index);
            prev_log_term = prev_entry.term;
        }

        auto request = std::make_unique<AppendEntriesRPC>(node_id_, node_id);
        request->term = state_->get_current_term();
        request->leader_id = node_id_;
        request->prev_log_index = prev_log_index;
        request->prev_log_term = prev_log_term;
        request->leader_commit = state_->get_commit_index();

        uint32_t last_index = log_storage_->get_last_index();
        uint32_t max_entries = config_.get_max_entries_per_request();

        if (next_index <= last_index)
        {
            uint32_t end_index = std::min(next_index + max_entries - 1, last_index);
            request->entries = log_storage_->get_entries(next_index, end_index);
        }

        logger_->debug("Sending AppendEntries to {} with {} entries (next_index={})",
                       node_id, request->entries.size(), next_index);
        outbox_.push_back(std::move(request));
    }

    void RaftNode::process_vote_response(const RequestVoteResponse &response)
    {
        std::lock_guard<std::mutex> lock(state_mutex_);

        if (response.term > state_->get_current_term())
        {
            become_follower(response.term);
            return;
        }

        if (!state_->is_candidate() || response.term != state_->get_current_term())
        {
            return;
        }

        state_->record_vote(response.source_node_id, response.vote_granted);

        logger_->debug("Received vote {} from {} (votes: {}/{})",
                       response.vote_granted ? "granted" : "denied",
                       response.source_node_id,
                       state_->get_vote_count(),
                       state_->get_majority_size());

        if (state_->has_majority_votes())
        {
            become_leader();
        }
    }

    void RaftNode::process_append_entries_response(int node_id, const AppendEntriesResponse &response)
    {
        std::lock_guard<std::mutex> lock(state_mutex_);

        if (response.term > state_->get_current_term())
        {
            become_follower(response.term);
            return;
        }

        if (!is_leader() || response.term != state_->get_current_term())
        {
            return;
        }

        if (response.success)
        {
            if (response.match_index > state_->get_match_index(node_id))
                state_->set_match_index(node_id, response.match_index);
            state_->set_next_index(node_id, state_->get_match_index(node_id) + 1);
            advance_commit_index();
        }
        else
        {
            uint32_t next = response.conflict_index;
            if (response.conflict_term > 0)
            {
                uint32_t first = log_storage_->get_first_index();
                for (uint32_t i = log_storage_->get_last_index(); i > 0 && i >= first; --i)
                {
                    uint32_t term = log_storage_->get_entry(i).term;
                    if (term == response.conflict_term)
                    {
                        next = i + 1;
                        break;
                    }
                    if (term < response.conflict_term)
                        break;
                }
            }
            state_->set_next_index(node_id, std::max(next, 1u));
        }
    }

    int RaftNode::generate_election_timeout()
    {
        return timeout_dist_(gen_);
    }

    void RaftNode::reset_election_timeout()
    {
        last_heartbeat_.store(std::chrono::steady_clock::now());
        election_timeout_ms_.store(generate_election_timeout());
    }

    bool RaftNode::should_grant_vote(const RequestVoteRPC &request) const
    {
        uint32_t voted_for = state_->get_voted_for();
        if (voted_for != 0 && voted_for != request.candidate_id)
        {
            return false;
        }

        uint32_t our_last_term = log_storage_->get_last_term();
        uint32_t our_last_index = log_storage_->get_last_index();

        if (request.last_log_term > our_last_term)
        {
            return true;
        }

        if (request.last_log_term == our_last_term && request.last_log_index >= our_last_index)
        {
            return true;
        }

        return false;
    }

    void RaftNode::advance_commit_index()
    {
        if (!is_leader())
            return;

        uint32_t current_commit = state_->get_commit_index();
        uint32_t last_index = log_storage_->get_last_index();

        for (uint32_t n = current_commit + 1; n <= last_index; ++n)
        {
            LogEntry entry = log_storage_->get_entry(n);
            if (entry.term != state_->get_current_term())
                continue;

            size_t replica_count = 1;
            for (int peer_id : peer_nodes_)
            {
                if (state_->get_match_index(peer_id) >= n)
                {
                    replica_count++;
                }
            }

            if (replica_count >= state_->get_majority_size())
            {
                state_->set_commit_index(n);
                logger_->debug("Advanced commit index to {}", n);
            }
        }

        apply_committed_entries();
    }

    void RaftNode::apply_committed_entries()
    {
        uint32_t last_applied = state_->get_last_applied();
        uint32_t commit_index = state_->get_commit_index();

        for (uint32_t i = last_applied + 1; i <= commit_index; ++i)
        {
            LogEntry entry = log_storage_->get_entry(i);

            if (entry.type == LogEntryType::CLIENT_COMMAND)
            {
                KVOperation op = KVOperation::from_log_entry(entry);
                logger_->debug("Applied entry {} - {} {}", i,
                               KVOperation::type_to_string(op.operation), op.key);
            }

            state_->set_last_applied(i);
        }
    }

    void RaftNode::handle_message(std::unique_ptr<Message> message)
    {
        switch (message->type)
        {
        case MessageType::REQUEST_VOTE:
        {
            auto request = static_cast<RequestVoteRPC *>(message.get());
            auto response = std::make_unique<RequestVoteResponse>(node_id_, request->source_node_id);
            response->message_id = request->message_id;

            handle_request_vote(*request, *response);
            network_manager_->send_message(request->source_node_id, std::move(response));
            break;
        }
        case MessageType::REQUEST_VOTE_RESPONSE:
        {
            auto response = static_cast<RequestVoteResponse *>(message.get());
            process_vote_response(*response);
            break;
        }
        case MessageType::APPEND_ENTRIES:
        {
            auto request = static_cast<AppendEntriesRPC *>(message.get());
            auto response = std::make_unique<AppendEntriesResponse>(node_id_, request->source_node_id);
            response->message_id = request->message_id;

            handle_append_entries(*request, *response);
            network_manager_->send_message(request->source_node_id, std::move(response));
            break;
        }
        case MessageType::APPEND_ENTRIES_RESPONSE:
        {
            auto response = static_cast<AppendEntriesResponse *>(message.get());
            process_append_entries_response(response->source_node_id, *response);
            break;
        }
        case MessageType::HEARTBEAT:
        {
            break;
        }
        default:
            logger_->warn("Received unknown message type: {}",
                          static_cast<int>(message->type));
            break;
        }
        flush_outbox();
    }

    void RaftNode::handle_connection_change(uint32_t node_id, bool connected)
    {
        if (connected)
        {
            logger_->info("Connected to node {}", node_id);
        }
        else
        {
            logger_->warn("Disconnected from node {}", node_id);
        }
    }

} // namespace raft