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

    void RaftNode::start()
    {
        std::lock_guard<std::mutex> lock(state_mutex_);

        if (running_.load())
        {
            return;
        }

        logger_->info("Starting Raft node {}", node_id_);

        if (!network_manager_->start())
        {
            throw std::runtime_error("Failed to start network manager");
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10000));

        running_.store(true);

        election_timer_thread_ = std::thread(&RaftNode::election_timer_thread, this);
        heartbeat_timer_thread_ = std::thread(&RaftNode::heartbeat_timer_thread, this);
        message_processor_thread_ = std::thread(&RaftNode::message_processor_thread, this);

        become_follower(state_->get_current_term());

        logger_->info("Raft node {} started successfully", node_id_);
    }

    void RaftNode::stop()
    {
        if (!running_.load())
        {
            return;
        }

        logger_->info("Stopping Raft node {}", node_id_);

        running_.store(false);
        state_cv_.notify_all();

        network_manager_->stop();

        if (election_timer_thread_.joinable())
            election_timer_thread_.join();
        if (heartbeat_timer_thread_.joinable())
            heartbeat_timer_thread_.join();
        if (message_processor_thread_.joinable())
            message_processor_thread_.join();

        persistent_state_->sync();
        log_storage_->sync();

        logger_->info("Raft node {} stopped", node_id_);
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

        logger_->debug("Received AppendEntries from {} for term {} with {} entries",
                       request.leader_id, request.term, request.entries.size());

        if (request.term < state_->get_current_term())
        {
            logger_->debug("Rejecting AppendEntries - stale term");
            return;
        }

        if (request.term > state_->get_current_term())
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
                logger_->debug("Log too short - missing entry {}", request.prev_log_index);
                return;
            }

            LogEntry prev_entry = log_storage_->get_entry(request.prev_log_index);
            if (prev_entry.term != request.prev_log_term)
            {
                response.conflict_term = prev_entry.term;
                response.conflict_index = request.prev_log_index;

                for (uint32_t i = request.prev_log_index; i >= log_storage_->get_first_index(); --i)
                {
                    LogEntry entry = log_storage_->get_entry(i);
                    if (entry.term != response.conflict_term)
                    {
                        response.conflict_index = i + 1;
                        break;
                    }
                }

                logger_->debug("Term mismatch at index {} - expected {}, got {}",
                               request.prev_log_index, request.prev_log_term, prev_entry.term);
                return;
            }
        }

        if (!request.entries.empty())
        {
            uint32_t append_index = request.prev_log_index + 1;
            if (log_storage_->has_entry(append_index))
            {
                LogEntry existing = log_storage_->get_entry(append_index);
                if (existing.term != request.entries[0].term)
                {
                    log_storage_->truncate_from(append_index);
                }
            }

            // Append new entries
            for (const auto &entry : request.entries)
            {
                if (!log_storage_->append_entry(entry))
                {
                    logger_->error("Failed to append entry {}", entry.index);
                    return;
                }
            }

            logger_->debug("Appended {} entries starting at index {}",
                           request.entries.size(), append_index);
        }

        if (request.leader_commit > state_->get_commit_index())
        {
            uint32_t new_commit = std::min(request.leader_commit, log_storage_->get_last_index());
            state_->set_commit_index(new_commit);
            logger_->debug("Updated commit index to {}", new_commit);
        }

        response.success = true;
        response.match_index = log_storage_->get_last_index();

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
        state_->set_current_term(term);
        state_->clear_voted_for();
        state_->clear_leader_id();

        persistent_state_->set_current_term(term);
        persistent_state_->clear_voted_for();

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
        logger_->info("Node {} becoming LEADER for term {}",
                      node_id_, state_->get_current_term());

        state_->set_state(NodeState::LEADER);
        state_->set_leader_id(node_id_);

        uint32_t last_log_index = log_storage_->get_last_index();
        state_->initialize_leader_state(last_log_index);

        send_append_entries();
    }

    void RaftNode::start_election()
    {
        logger_->info("Starting election for term {} - peer_nodes_.size() = {}",
                      state_->get_current_term(), peer_nodes_.size());

        if (peer_nodes_.empty())
        {
            logger_->warn("No peers configured - cannot send RequestVote messages!");
            return;
        }

        for (int peer_id : peer_nodes_)
        {
            logger_->info("Sending RequestVote to peer {}", peer_id);
            auto request = std::make_unique<RequestVoteRPC>(node_id_, peer_id);
            request->term = state_->get_current_term();
            request->candidate_id = node_id_;
            request->last_log_index = log_storage_->get_last_index();
            request->last_log_term = log_storage_->get_last_term();

            // Try sending with a small retry
            bool sent = false;
            for (int retry = 0; retry < 3 && !sent; retry++)
            {
                sent = network_manager_->send_message(peer_id, std::move(request));
                if (!sent && retry < 2)
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    // Recreate the request for retry
                    request = std::make_unique<RequestVoteRPC>(node_id_, peer_id);
                    request->term = state_->get_current_term();
                    request->candidate_id = node_id_;
                    request->last_log_index = log_storage_->get_last_index();
                    request->last_log_term = log_storage_->get_last_term();
                }
            }
            logger_->info("RequestVote send result to peer {}: {}", peer_id, sent ? "SUCCESS" : "FAILED");
        }
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

        network_manager_->send_message(node_id, std::move(request));
    }

    void RaftNode::process_vote_response(const RequestVoteResponse &response)
    {
        std::lock_guard<std::mutex> lock(state_mutex_);

        if (!state_->is_candidate() || response.term != state_->get_current_term())
        {
            return;
        }

        if (response.term > state_->get_current_term())
        {
            become_follower(response.term);
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

        if (!is_leader() || response.term != state_->get_current_term())
        {
            return;
        }

        if (response.term > state_->get_current_term())
        {
            become_follower(response.term);
            return;
        }

        if (response.success)
        {
            // Update next_index and match_index
            state_->set_match_index(node_id, response.match_index);
            state_->set_next_index(node_id, response.match_index + 1);

            logger_->debug("AppendEntries success from {} (match_index={})",
                           node_id, response.match_index);

            advance_commit_index();
        }
        else
        {
            if (response.conflict_term > 0)
            {
                uint32_t new_next_index = response.conflict_index;
                for (uint32_t i = response.conflict_index; i >= log_storage_->get_first_index(); --i)
                {
                    LogEntry entry = log_storage_->get_entry(i);
                    if (entry.term == response.conflict_term)
                    {
                        new_next_index = i + 1;
                        break;
                    }
                }
                state_->set_next_index(node_id, new_next_index);
            }
            else
            {
                state_->set_next_index(node_id, response.conflict_index);
            }

            logger_->debug("AppendEntries failed from {} - new next_index={}",
                           node_id, state_->get_next_index(node_id));

            send_append_entries_to_node(node_id);
        }
    }

    void RaftNode::election_timer_thread()
    {
        while (running_.load())
        {
            std::unique_lock<std::mutex> lock(state_mutex_);

            auto timeout = std::chrono::milliseconds(election_timeout_ms_.load());
            auto deadline = last_heartbeat_.load() + timeout;

            if (state_cv_.wait_until(lock, deadline) == std::cv_status::timeout)
            {
                if (!is_leader() && running_.load())
                {
                    logger_->debug("Election timeout - starting new election");
                    become_candidate();
                }
            }
        }
    }

    void RaftNode::heartbeat_timer_thread()
    {
        while (running_.load())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(HEARTBEAT_INTERVAL_MS));

            if (is_leader() && running_.load())
            {
                send_append_entries();
            }
        }
    }

    void RaftNode::message_processor_thread()
    {
        // This would typically process messages from a queue
        // For this implementation, messages are handled directly in callbacks
        while (running_.load())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
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
            reset_election_timeout();
            break;
        }
        default:
            logger_->warn("Received unknown message type: {}",
                          static_cast<int>(message->type));
            break;
        }
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