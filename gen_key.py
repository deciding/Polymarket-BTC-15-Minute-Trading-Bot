from py_clob_client_v2 import ClobClient

# 替换为你刚才从 MetaMask 导出的 64 位私钥
YOUR_PRIVATE_KEY = "" 

# 初始化 L1 客户端（Polygon主网链ID为137）
client = ClobClient(
    host="https://clob.polymarket.com",
    chain_id=137,
    key=YOUR_PRIVATE_KEY
)

# 核心：生成或衍生 L2 凭证
# 此时返回的是一个 ApiCreds 类型的对象
creds = client.create_or_derive_api_key()

print("--- 复制以下参数填入 NautilusTrader 配置 ---")
# 📢 注意：新版类对象的属性名带有前缀 api_，需按如下方式读取
print(f"api_key:    {creds.api_key}")
print(f"api_secret: {creds.api_secret}")
print(f"passphrase: {creds.api_passphrase}")
