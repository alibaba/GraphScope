#pragma once

#include <ostream>
#include <string>

namespace pthash {

struct progress_logger {
    progress_logger(uint64_t total_events, std::string const& prefix = "",
                    std::string const& suffix = "", bool enable = true)
        : m_total_events(total_events), m_prefix(prefix), m_suffix(suffix), m_logged_events(0) {
        // TODO: improve the computation of log_step using timings !
        uint64_t perc_fraction = (total_events >= 100000000) ? 100 : 20;
        m_log_step = (total_events + perc_fraction - 1) / perc_fraction;
        m_next_event_to_log = static_cast<uint64_t>(-1);
        if (enable) {
            m_next_event_to_log = m_log_step;
            update(false);
        }
    }

    inline void log() {
        if (++m_logged_events >= m_next_event_to_log) {
            update(false);
            m_next_event_to_log += m_log_step;
            // the following ensures the last update on 100%
            if (m_next_event_to_log > m_total_events) m_next_event_to_log = m_total_events;
        }
    }

    void finalize() {
        if (m_next_event_to_log != static_cast<uint64_t>(-1)) {
            assert(m_next_event_to_log == m_total_events);
            assert(m_logged_events == m_total_events);
            update(true);
        }
    }

    uint64_t total_events() const {
        return m_total_events;
    }

    uint64_t logged_events() const {
        return m_logged_events;
    }

private:
    inline void update(bool final) const {
        uint64_t perc = (100 * m_logged_events / m_total_events);
        std::cout << "\r" << m_prefix << perc << "%" << m_suffix;
        if (final) {
            std::cout << std::endl;
        } else {
            std::cout << std::flush;
        }
    }

    const uint64_t m_total_events;
    const std::string m_prefix = "";
    const std::string m_suffix = "";
    uint64_t m_logged_events;
    uint64_t m_log_step;
    uint64_t m_next_event_to_log;
};

}  // namespace pthash