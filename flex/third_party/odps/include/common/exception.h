/** Apsara ExceptionBase
 * All exception classes in Apsara should inherit ExceptionBase
 */

/**
 * Guidelines for exception handling
 * 1. How to throw
 *    APSARA_THROW(the_exception_to_show, description_string)
 *    e.g. APSARA_THROW(ParameterInvalidException, "Cannot be empty string");
 *    If you throw not using APSARA_THROW, the location of throw will not be
 * recorded.
 *
 * 2. Define an new apsara exception:
 *    a. SHOULD inherit from apsara::ExceptionBase or its sub-classes.
 *    b. SHOULD use macro APSARA_DEFINE_EXCEPTION inside public: region of the
 * class definition.
 *
 * 3. GetClassName(): exception class name.
 *    GetMessage(): (virtual) One can customize it to return some specific
 * details. By default, it returns the description string when you construct the
 * exception. GetStackTrace(): call stack when the exception is thrown.
 *    ToString(): return a string describing:
 *        a) where the exception is thrown (if available)
 *        b) the exception class name
 *        c) the content of GetMessage
 *        d) stack trace
 *        e) cause, if available
 *    what(): Same as ToString(). But the return type is const char *.
 *    GetExceptionChain(): Same as ToString(). Kept for backward compatibility.
 */

#ifndef APSARA_EXCEPTION_H
#define APSARA_EXCEPTION_H

#include <exception>
#include <memory>
#include <string>

#define APSARA_THROW(ExClass, args...)                      \
  do {                                                      \
    ExClass tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0(args); \
    tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0.Init(          \
        __FILE__, __PRETTY_FUNCTION__, __LINE__);           \
    throw tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0;         \
  } while (false)

#define APSARA_THROW_CHAIN(ExClass, cause, args...)           \
  do {                                                        \
    ExClass tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0(args);   \
    tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0.Init(            \
        __FILE__, __PRETTY_FUNCTION__, __LINE__);             \
    tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0.SetCause(cause); \
    throw tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0;           \
  } while (false)

#define APSARA_THROW_LOG(logger, additionalInfo, ExClass, args...)             \
  do {                                                                         \
    ExClass tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0(args);                    \
    tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0.Init(                             \
        __FILE__, __PRETTY_FUNCTION__, __LINE__);                              \
    LOG_INFO(                                                                  \
        logger,                                                                \
        ("ExceptionClass", #ExClass)(                                          \
            "Message", tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0.GetMessage())( \
            "CallStack",                                                       \
            tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0.GetStackTrace())(         \
            "What", tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0.ToString())       \
            additionalInfo);                                                   \
    throw tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0;                            \
  } while (false)

#define APSARA_THROW_CHAIN_LOG(logger, additionalInfo, ExClass, cause,         \
                               args...)                                        \
  do {                                                                         \
    ExClass tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0(args);                    \
    tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0.Init(                             \
        __FILE__, __PRETTY_FUNCTION__, __LINE__);                              \
    tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0.SetCause(cause);                  \
    LOG_INFO(                                                                  \
        logger,                                                                \
        ("ExceptionClass", #ExClass)(                                          \
            "Message", tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0.GetMessage())( \
            "CallStack",                                                       \
            tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0.GetStackTrace())(         \
            "What", tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0.ToString())       \
            additionalInfo);                                                   \
    throw tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0;                            \
  } while (false)

#define APSARA_THROW_IF(condition, ExClass, args...) \
  do {                                               \
    bool cond = (condition);                         \
    if (cond) {                                      \
      APSARA_THROW(ExClass, args);                   \
    }                                                \
  } while (false)

#define APSARA_DEFINE_EXCEPTION(ExClass, Base)                         \
  ExClass() throw() {}                                                 \
  ExClass(const std::string& strMsg) throw() { mMessage = strMsg; }    \
                                                                       \
  ~ExClass() throw() {}                                                \
                                                                       \
  /* override */ std::string GetClassName() const { return #ExClass; } \
                                                                       \
  /* override */ std::shared_ptr<ExceptionBase> Clone() const {        \
    return std::shared_ptr<ExceptionBase>(new ExClass(*this));         \
  }

namespace apsara {

class Any;

class ExceptionBase : public std::exception {
 public:
  ExceptionBase(const std::string& message = "") throw();

  virtual ~ExceptionBase() throw();

  virtual std::shared_ptr<ExceptionBase> Clone() const;

  void Init(const char* file, const char* function, int line);

  virtual void SetCause(const ExceptionBase& cause);

  virtual void SetCause(std::shared_ptr<ExceptionBase> cause);

  virtual std::shared_ptr<ExceptionBase> GetCause() const;

  // Return the root cause, if the exception has the root cause; else return
  // itself
  virtual std::shared_ptr<ExceptionBase> GetRootCause() const;

  virtual std::string GetClassName() const;

  virtual std::string GetMessage() const;

  /**
   * With a) detailed throw location (file + lineno) (if availabe), b) Exception
   * class name, and c) content of GetMessage() (if not empty)
   */
  /* override */ const char* what() const throw();

  /**
   * Synonym of what(), except for the return type.
   */
  const std::string& ToString() const;

  const std::string& GetExceptionChain() const;

  std::string GetStackTrace() const;

 protected:
  std::shared_ptr<ExceptionBase> mNestedException;
  std::string mMessage;
  std::string mFile;
  std::string mFunction;
  int mLine;

 private:
  enum { MAX_STACK_TRACE_SIZE = 50 };
  void* mStackTrace[MAX_STACK_TRACE_SIZE];
  size_t mStackTraceSize;

  mutable std::string mWhat;

  friend Any ToJson(const ExceptionBase& e);
  friend void FromJson(ExceptionBase& e, const Any& a);
};

class InvalidOperation : public ExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(InvalidOperation, ExceptionBase);
};

class RuntimeError : public ExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(RuntimeError, ExceptionBase);
};

class TimeoutError : public ExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(TimeoutError, ExceptionBase);
};

class LogicError : public ExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(LogicError, ExceptionBase);
};

class OverflowError : public ExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(OverflowError, ExceptionBase);
};

class AlreadyExistException : public ExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(AlreadyExistException, ExceptionBase);
};

class NotExistException : public ExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(NotExistException, ExceptionBase);
};

class NotImplementedException : public ExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(NotImplementedException, ExceptionBase);
};

// parameter invalid
class ParameterInvalidException : public virtual ExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(ParameterInvalidException, ExceptionBase);
};

// operation is denied
class AuthenticationFailureException : public ExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(AuthenticationFailureException, ExceptionBase);
};

// base class for all exceptions in storage system
class StorageExceptionBase : public virtual ExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(StorageExceptionBase, ExceptionBase);
};

// when create an exist file
class FileExistException : public StorageExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(FileExistException, StorageExceptionBase);
};

// when open/delete/rename/... an non-exist file
class FileNotExistException : public StorageExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(FileNotExistException, StorageExceptionBase);
};

class DirectoryExistException : public StorageExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(DirectoryExistException, StorageExceptionBase);
};

class DirectoryNotExistException : public StorageExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(DirectoryNotExistException, StorageExceptionBase);
};

class SameNameEntryExistException : public StorageExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(SameNameEntryExistException, StorageExceptionBase);
};

// when append/delete a file that being appended
class FileAppendingException : public StorageExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(FileAppendingException, StorageExceptionBase);
};

// when opening a file that cannot be overwritten
class FileOverwriteException : public StorageExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(FileOverwriteException, StorageExceptionBase);
};

// when append/delete a file that being appended
class PangunNotEnoughChunkserverExcepion : public StorageExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(PangunNotEnoughChunkserverExcepion,
                          StorageExceptionBase);
};

// when read, data is unavailable due to internal error
class DataUnavailableException : public StorageExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(DataUnavailableException, StorageExceptionBase);
};

// when append/commit, data stream is corrupted due to internal error
class StreamCorruptedException : public StorageExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(StreamCorruptedException, StorageExceptionBase);
};

// when end of stream comes unexpected
class UnexpectedEndOfStreamException : public StorageExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(UnexpectedEndOfStreamException, StorageExceptionBase);
};

// base class for all exceptions in service
class ServiceExceptionBase : public virtual ExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(ServiceExceptionBase, ExceptionBase);
};

// can't get service connection
class ServiceUnavailableException : public ServiceExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(ServiceUnavailableException, ServiceExceptionBase);
};

// internal server error
class InternalServerErrorException : public ServiceExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(InternalServerErrorException, ServiceExceptionBase);
};

// session expired
class SessionExpireException : public ServiceExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(SessionExpireException, ServiceExceptionBase);
};

// running into unimplemented code
class UnimplementedException : public virtual ExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(UnimplementedException, ExceptionBase);
};

/** Base Exception of Kuafu component
 */
class KuafuExceptionBase : public ExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(KuafuExceptionBase, ExceptionBase)
};

/** Expection class used in Kuafu component
 */
class KuafuNetworkException : public KuafuExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(KuafuNetworkException, KuafuExceptionBase)
};

/** Expection class which is thrown when reading on a closed RpcPort
 */
class KuafuRpcPortClosedException : public KuafuExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(KuafuRpcPortClosedException, KuafuExceptionBase)
};

/** alias of Kuafu exceptions to be compatible with previous version
 */
namespace kuafu {
class KuafuException : public apsara::KuafuNetworkException {
 public:
  APSARA_DEFINE_EXCEPTION(KuafuException, apsara::KuafuNetworkException)
};

class RpcPortClosedException : public apsara::KuafuRpcPortClosedException {
 public:
  APSARA_DEFINE_EXCEPTION(RpcPortClosedException,
                          apsara::KuafuRpcPortClosedException)
};

class ConnectionClosedException : public apsara::KuafuExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(ConnectionClosedException, apsara::KuafuExceptionBase)
};

class MessageDroppedException : public apsara::KuafuExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(MessageDroppedException, apsara::KuafuExceptionBase)
};

class NoBufferException : public apsara::KuafuExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(NoBufferException, apsara::KuafuExceptionBase)
};

class BufferGrabbedException : public apsara::KuafuExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(BufferGrabbedException, apsara::KuafuExceptionBase)
};

class KfcConnectionClosedException : public apsara::kuafu::KuafuException {
 public:
  APSARA_DEFINE_EXCEPTION(KfcConnectionClosedException,
                          apsara::kuafu::KuafuException)
};

class ServerNotExistException : public apsara::kuafu::KuafuException {
 public:
  APSARA_DEFINE_EXCEPTION(ServerNotExistException,
                          apsara::kuafu::KuafuException)
};
}  // namespace kuafu
}  // namespace apsara

#include <cxxabi.h>
#include <execinfo.h>
#include <stdlib.h>

namespace apsara {
std::string operator+(const std::string& lhs, int);

inline ExceptionBase::ExceptionBase(const std::string& message) throw()
    : mMessage(message),
      mFile("<unknown file>"),
      mFunction("<unknown function>"),
      mLine(-1),
      mStackTraceSize(0) {}

inline ExceptionBase::~ExceptionBase() throw() {}

inline std::shared_ptr<ExceptionBase> ExceptionBase::Clone() const {
  return std::shared_ptr<ExceptionBase>(new ExceptionBase(*this));
}

inline void ExceptionBase::Init(const char* file, const char* function,
                                int line) {
  mFile = file;
  mFunction = function;
  mLine = line;
  mStackTraceSize = backtrace(mStackTrace, MAX_STACK_TRACE_SIZE);
}

inline void ExceptionBase::SetCause(const ExceptionBase& cause) {
  SetCause(cause.Clone());
}

inline void ExceptionBase::SetCause(std::shared_ptr<ExceptionBase> cause) {
  mNestedException = cause;
}

inline std::shared_ptr<ExceptionBase> ExceptionBase::GetCause() const {
  return mNestedException;
}

inline std::shared_ptr<ExceptionBase> ExceptionBase::GetRootCause() const {
  if (mNestedException.get()) {
    std::shared_ptr<ExceptionBase> rootCause = mNestedException->GetRootCause();
    if (rootCause.get())
      return rootCause;
  }
  return mNestedException;
}

inline std::string ExceptionBase::GetClassName() const {
  return "ExceptionBase";
}

inline std::string ExceptionBase::GetMessage() const { return mMessage; }

inline const char* ExceptionBase::what() const throw() {
  return ToString().c_str();
}

inline const std::string& ExceptionBase::ToString() const {
  if (mWhat.empty()) {
    if (mLine > 0)
      mWhat = std::string(mFile) + "(" + std::to_string(mLine) + ")";
    else
      mWhat = "<unknown throw location>";
    mWhat += ": " + GetClassName();
    std::string customizedString = GetMessage();
    if (!customizedString.empty()) {
      mWhat += ": " + customizedString;
    }
    mWhat += "\nStack trace:\n";
    mWhat += GetStackTrace();
    if (mNestedException.get()) {
      mWhat += "Caused by:\n" + mNestedException->ToString();
    }
  }
  return mWhat;
}

inline const std::string& ExceptionBase::GetExceptionChain() const {
  return ToString();
}

inline std::string ExceptionBase::GetStackTrace() const {
  if (mStackTraceSize == 0)
    return "<No stack trace>\n";
  char** strings = backtrace_symbols(mStackTrace, mStackTraceSize);
  if (strings == NULL)  // Since this is for debug only thus
                        // non-critical, don't throw an exception.
    return "<Unknown error: backtrace_symbols returned NULL>\n";

  std::string result;
  for (size_t i = 0; i < mStackTraceSize; ++i) {
    std::string mangledName = strings[i];
    std::string::size_type begin = mangledName.find('(');
    std::string::size_type end = mangledName.find('+', begin);
    if (begin == std::string::npos || end == std::string::npos) {
      result += mangledName;
      result += '\n';
      continue;
    }
    ++begin;
    int status;
    char* s = abi::__cxa_demangle(
        mangledName.substr(begin, end - begin).c_str(), NULL, 0, &status);
    if (status != 0) {
      result += mangledName;
      result += '\n';
      continue;
    }
    std::string demangledName(s);
    free(s);
    // Ignore ExceptionBase::Init so the top frame is the
    // user's frame where this exception is thrown.
    //
    // Can't just ignore frame#0 because the compiler might
    // inline ExceptionBase::Init.
    if (i == 0 &&
        demangledName ==
            "apsara::ExceptionBase::Init(char const*, char const*, int)")
      continue;
    result += mangledName.substr(0, begin);
    result += demangledName;
    result += mangledName.substr(end);
    result += '\n';
  }
  free(strings);
  return result;
}

}  // namespace apsara

#endif  // APSARA_EXCEPTION_H
