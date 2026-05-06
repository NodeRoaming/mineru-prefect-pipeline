from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, SecretStr


class MineruConfig(BaseSettings):
    apply_batch_url: str = Field(default="https://mineru.net/api/v4/file-urls/batch")
    """批量获取文件申请链接的URL"""

    extract_results_url: str = Field(default="https://mineru.net/api/v4/extract-results/batch")
    """获取解析结果的URL"""

    model_version: str = Field(default="vlm")
    """使用的模型"""

    api_token: SecretStr | None = Field(default=None, alias="mineru_api_token")
    """MinerU API Token"""

    model_config = SettingsConfigDict(env_file=".env")
    """MinerU Model Config"""


config = MineruConfig()
