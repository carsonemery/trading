#pragma once

#include "Common.h"
#include <memory>

namespace IBTrading {

class Logger {
public:
    enum class Level {
        DEBUG,
        INFO,
        WARN,
        ERROR
    };

    static Logger& getInstance();
    
    void setLevel(Level level);
    void log(Level level, const std::string& message);
    void debug(const std::string& message);
    void info(const std::string& message);
    void warn(const std::string& message);
    void error(const std::string& message);

private:
    Logger() = default;
    Level currentLevel_ = Level::INFO;
    std::string getLevelString(Level level) const;
    std::string getTimestamp() const;
};

} // namespace IBTrading
