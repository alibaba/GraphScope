#pragma once

#ifndef APSARA_ODPS_SDK_CONFIGURATION_H
#define APSARA_ODPS_SDK_CONFIGURATION_H

#include <memory>
#include <string>
#include "common/string_tools.h"
namespace apsara {
namespace odps {
namespace sdk {

/**
 *	@brief 账号类型.
 */
// 阿里云账号
const char* const ACCOUNT_ALIYUN = "aliyun";
// sts token
const char* const ACCOUNT_STS = "sts";
// 访问令牌
const char* const ACCOUNT_TOKEN = "token";
// 域账号
const char* const ACCOUNT_DOMAIN = "domain";
// 淘宝账号
const char* const ACCOUNT_TAOBAO = "taobao";
// 应用签名
const char* const ACCOUNT_APPLICATION = "app";

/**
 * @class Account
 *
 * @brief 保存了访问账号相关信息的类.
 */
class Account {
 protected:
  std::string type;
  std::string token; /**< 访问令牌*/

  std::string id;  /**< 访问的账号名*/
  std::string key; /**< 访问的账号秘钥*/

  std::string applicationSignature; /**< 应用签名*/
  /**
   *	@brief 签名算法名称.
   *
   *	目前淘宝帐号有rsa\hamc-sha1两种,
   *云帐号只支持hamc-sha1，域帐号不支持客户端签名
   */
  std::string algorithm;

 public:
  /**
   *	@brief 构造函数.
   *
   *	签名算法默认为hmac-sha1
   */
  Account() : algorithm("hmac-sha1") {}

  /**
   *	@brief 构造函数.
   *
   *	签名算法默认为hmac-sha1
   *	@param type	账号类型
   *	@param token 访问令牌
   */
  Account(std::string _type, std::string _token)
      : type(_type), token(_token), algorithm("hmac-sha1") {}

  /**
   *	@brief 构造函数.
   *
   *	签名算法默认为hmac-sha1
   *	@param type	账号类型
   *	@param id 访问的账户名
   *	@param key 访问的账户秘钥
   */
  Account(std::string _type, std::string _id, std::string _key)
      : type(_type), id(_id), key(_key), algorithm("hmac-sha1") {}

  virtual ~Account() {}

  virtual std::string GetType() const { return this->type; }

  virtual std::string GetId() const { return this->id; }

  virtual std::string GetKey() const { return this->key; }

  void SetId(const std::string& id) { this->id = id; }

  void SetKey(const std::string& key) { this->key = key; }

  virtual std::string GetToken() const { return this->token; }

  virtual std::string GetApplicationSignature() const {
    return this->applicationSignature;
  }

  virtual std::string GetAlgorithm() const { return this->algorithm; }

  virtual void SetApplicationSignature(std::string applicationSignature) {
    this->applicationSignature = applicationSignature;
  }

  virtual void SetAlgorithm(std::string algorithm) {
    this->algorithm = algorithm;
  }

  /**
   *	@brief 是否有效.
   *这个函数不会发请求到服务器，仅检查需要的字段是否已填充。
   */
  virtual bool IsValid() const { return true; }
};

typedef std::shared_ptr<Account> AccountPtr;

class AliyunAccount : public Account {
 public:
  AliyunAccount(std::string access_id, std::string access_key)
      : Account(ACCOUNT_ALIYUN, access_id, access_key) {}

  bool IsValid() const override { return id.size() != 0 && key.size() != 0; }
};

class AppAccount : public Account {
 public:
  AppAccount() : Account(ACCOUNT_APPLICATION, "", "") {}
  AppAccount(const Account& account)
      : Account(ACCOUNT_APPLICATION, account.GetId(), account.GetKey()) {}

  AppAccount(const std::string& access_id, const std::string& access_key)
      : Account(ACCOUNT_APPLICATION, access_id, access_key) {}

  bool IsValid() const override { return id.size() != 0 && key.size() != 0; }
};

class StsToken : public Account {
 public:
  StsToken() : Account(ACCOUNT_STS, "") {}
  StsToken(const Account& account) : Account(ACCOUNT_STS, account.GetToken()) {}
  StsToken(const std::string& stsToken) : Account(ACCOUNT_STS, stsToken) {}

  bool IsValid() const override { return token.size() != 0; }
};

typedef std::shared_ptr<AppAccount> AppAccountPtr;

/**
 * @class Configuration
 *
 * @brief 账号和访问的相关配置.
 */
class Configuration {
 public:
  static const int DEFAULT_CHUNK_SIZE =
      1500 - 4; /**< HTTP chunked编码的默认chunk size*/
  static const int DEFAULT_SOCKET_CONNECT_TIMEOUT =
      180; /**< Socket连接超时默认时间，单位是秒*/
  static const int DEFAULT_SOCKET_TIMEOUT =
      300; /**< Socket超时默认时间，单位是秒*/

  Account account;       /**< Account类*/
  AppAccount appAccount; /**< AppAccount类，用于双签名*/
  StsToken stsToken;

  std::string accessId;       /**< 访问账户的ID*/
  std::string accessKey;      /**< 访问账户的秘钥*/
  std::string tunnelEndpoint; /**< 关闭路由功能时的tunnel endpoint*/
  std::string quotaName;      /**< 访问账户的quota name>*/
  std::string regionId;       /**< 内部请求所在的regionID*/

  int chunkSize;            /**< HTTP chunked编码的chunk size*/
  int socketConnectTimeout; /**< Socket连接超时时间*/
  int socketTimeout;        /**< Socket超时时间*/
  bool disableSSLVerify;    /**< 只有在测试无CA证书服务器时使用*/
  std::string odpsEndpoint; /**< 开启路由功能时的ODPS endpoint*/
  std::string defaultProject;

  std::string userAgent;

  /**
   *	@brief 构造函数.
   *
   *	chunk size设置为默认值
   *	socket连接超时设置为默认值
   *	socket超时设置为默认值
   *	SSL验证默认开启
   */
  Configuration();

  /**
   *	@brief 构造函数.
   *
   *	SSL验证默认开启
   *	@param account 账号
   *	@param endpoint 访问终端地址
   */
  Configuration(Account account, const std::string& endpoint);

  /**
   *	@brief 成员变量account的getter函数.
   *
   *	@return 返回account成员变量的一个拷贝
   */
  const Account& GetAccount() const { return this->account; }

  const AppAccount& GetAppAccount() const { return this->appAccount; }

  void SetAppAccount(AppAccount app) { this->appAccount = app; }

  /**
   *	@brief 设置ODPS tunnelEndpoint并启用tunnel路由功能
   *
   *	@param ODPS tunnelEndpoint
   *	@return 无返回值
   */
  void SetTunnelEndpoint(const std::string& ep) { this->tunnelEndpoint = ep; }

  /**
   *	@brief 成员变量tunnelEndpoint的getter函数.
   *
   *	@return 返回tunnelEndpoint成员变量的一个拷贝
   */
  const std::string& GetTunnelEndpoint() { return this->tunnelEndpoint; }

  /**
   *	@brief 设置quotaName
   *
   *	@param quotaName quota name
   *	@return 无返回值
   */
  void SetQuotaName(const std::string& quotaName) {
    this->quotaName = quotaName;
  }

  /**
   *	@brief 成员变量quotaName的getter函数.
   *
   *	@return 返回quotaName成员变量的一个拷贝
   */
  const std::string& GetQuotaName() { return this->quotaName; }

  /**
   *	@brief 成员变量account的setter函数.
   *
   *	@param account Account类的引用
   *	@return 无返回值
   */
  void SetAccount(const Account& account) { this->account = account; }

  /**
   *	@brief 设置 STS Token
   *
   *	@param tok STS Token
   *	@return 无返回值
   */
  void SetStsToken(const StsToken& tok) { this->stsToken = tok; }

  const StsToken& GetStsToken() const { return this->stsToken; }

  /**
   *	@brief 成员变量odpsEndpoint的getter函数.
   *
   *	@return 返回odpsEndpoint成员变量的一个拷贝
   */
  const std::string& GetEndpoint() const { return odpsEndpoint; }

  /**
   *	@brief 成员变量odpsEndpoint的setter函数.
   *
   *	@param endpoint 标准string类的引用
   *	@return 无返回值
   */
  void SetEndpoint(const std::string& endpoint) {
    this->odpsEndpoint = endpoint;
  }

  /**
   *	@brief 成员变量chunkSize的getter函数.
   *
   *	@return 返回chunk的大小
   */
  int GetChunkSize() { return chunkSize; }

  /**
   *	@brief 成员变量chunkSize的setter函数.
   *
   *	@param chunkSize chunk的大小
   *	@return 无返回值
   */
  void SetChunkSize(int chunkSize) { this->chunkSize = chunkSize; }

  /**
   *	@brief 成员变量socketConnectTimeout的getter函数.
   *
   *	@return 返回socket连接超时的时间阈值
   */
  int GetSocketConnectTimeout() { return socketConnectTimeout; }

  /**
   *	@brief 成员变量socketConnectTimeout的setter函数.
   *
   *	@param timeout socket连接超时的时间阈值
   *	@return 无返回值
   */
  void SetSocketConnectTimeout(int timeout) {
    this->socketConnectTimeout = timeout;
  }

  /**
   *	@brief 成员变量socketTimeout的getter函数.
   *
   *	@return 返回socket超时的时间阈值
   */
  int GetSocketTimeout() { return socketTimeout; }

  /**
   *	@brief 成员变量socketTimeout的setter函数.
   *
   *	@param timeout socket超时的时间阈值，单位是秒
   *	@return 无返回值
   */
  void SetSocketTimeout(int timeout) { this->socketTimeout = timeout; }

  /**
   *	@brief 成员变量defaultProject的getter函数.
   */
  const std::string& GetDefaultProject() { return defaultProject; }

  /**
   *	@brief 成员变量defaultProject的setter函数.
   */
  void SetDefaultProject(const std::string& project) {
    this->defaultProject = project;
  }

  /**
   *	@brief 未实现.
   */
  void loadConfig(const std::string& resource) {}

#ifdef ENABLE_VIPSERVER
  bool useVIPServer;
  /**
   * @brief 成员变量useVIPServer的setter函数, 仅在vipserver模式编译下有效
   */
  void SetUseVIPServer(bool useVIP) { useVIPServer = useVIP; }

  /**
   * @brief 成员变量useVIPServer的getter函数.
   */
  bool IsUseVIPServer() const { return useVIPServer; }
#endif
  /**
   *	@brief 成员变量regionId的getter函数.
   *
   *	@return 返回regionId成员变量的一个拷贝
   */
  std::string GetRegionId() { return regionId; }

  /**
   *	@brief 成员变量regionId的setter函数.
   *
   *	@param regionId 标准string类的引用
   *	@return 无返回值
   */
  void SetRegionId(const std::string& regionId) { this->regionId = regionId; }

  /**
   *	@brief 成员变量userAgent的setter函数.
   *
   *	@param userAgent 用户标识
   *	@return 无返回值
   */
  void SetUserAgent(const std::string& userAgent) {
    this->userAgent = userAgent;
  }

  /**
   *	@brief 成员变量userAgent的setter函数.
   *
   *	@param userAgent 用户标识
   *	@return 无返回值
   */
  std::string GetUserAgent() const { return userAgent; }
};

typedef std::shared_ptr<Configuration> ConfigurationPtr;

}  // namespace sdk
}  // namespace odps
}  // namespace apsara

namespace apsara {
namespace odps {
namespace sdk {

inline Configuration::Configuration()
    : chunkSize(DEFAULT_CHUNK_SIZE),
      socketConnectTimeout(DEFAULT_SOCKET_CONNECT_TIMEOUT),
      socketTimeout(DEFAULT_SOCKET_TIMEOUT),
      disableSSLVerify(false) {
#ifdef ENABLE_VIPSERVER
  useVIPServer = true;
#endif
}

inline Configuration::Configuration(Account _account,
                                    const std::string& _endpoint)
    : account(_account),
      chunkSize(DEFAULT_CHUNK_SIZE),
      socketConnectTimeout(DEFAULT_SOCKET_CONNECT_TIMEOUT),
      socketTimeout(DEFAULT_SOCKET_TIMEOUT),
      disableSSLVerify(false),
      odpsEndpoint(_endpoint) {
  if (!apsara::odps::sdk::storage_api::StartWith(odpsEndpoint, "http://") &&
      !apsara::odps::sdk::storage_api::StartWith(odpsEndpoint, "https://")) {
    throw std::exception();
  }
#ifdef ENABLE_VIPSERVER
  useVIPServer = true;
#endif
}

}  // namespace sdk
}  // namespace odps
}  // namespace apsara
#endif
