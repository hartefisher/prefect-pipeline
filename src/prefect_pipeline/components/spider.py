from collections.abc import AsyncGenerator
from functools import cached_property
from typing import Any, TypedDict, Unpack, cast
from urllib.parse import parse_qsl, urlparse

from bs4 import BeautifulSoup
from httpx import AsyncClient, Response

from prefect_pipeline.components.helper import AutoItemModel, ExtraContextMixin, PipelineHelper
from prefect_pipeline.infra.exceptions import ResultIsEmpty
from prefect_pipeline.infra.utils import TimeStatistic
from prefect_pipeline.models import BaseItem


# crawl4ai / playwright 为浏览器爬虫的可选依赖（prefect_pipeline[cdp] extra），
# 此处仅用 Any 描述配置形状，避免模块级硬依赖；具体类型在使用方（业务项目）中解析。
class BrowserConfigDict(TypedDict, total=False):
    browser_type: str
    headless: bool
    browser_mode: str
    use_managed_browser: bool
    cdp_url: str | None
    use_persistent_context: bool
    user_data_dir: str | None
    chrome_channel: str
    channel: str
    proxy: str | None
    proxy_config: Any
    viewport_width: int
    viewport_height: int
    viewport: dict[str, int] | None
    accept_downloads: bool
    downloads_path: str | None
    storage_state: Any
    ignore_https_errors: bool
    java_script_enabled: bool
    sleep_on_close: bool
    verbose: bool
    cookies: list[dict[str, Any]] | None
    headers: dict[str, str] | None
    user_agent: str
    user_agent_mode: str
    user_agent_generator_config: dict[str, Any]
    text_mode: bool
    light_mode: bool
    extra_args: list[str] | None
    debugging_port: int
    host: str


class CrawlerRunConfigDict(TypedDict, total=False):
    word_count_threshold: int
    extraction_strategy: Any
    chunking_strategy: Any
    markdown_generator: Any
    only_text: bool
    css_selector: str | None
    target_elements: list[str] | None
    excluded_tags: list[str] | None
    excluded_selector: str | None
    keep_data_attributes: bool
    keep_attrs: list[str] | None
    remove_forms: bool
    prettiify: bool
    parser_type: str
    scraping_strategy: Any
    proxy_config: Any
    proxy_rotation_strategy: Any
    locale: str | None
    timezone_id: str | None
    geolocation: Any
    fetch_ssl_certificate: bool
    cache_mode: Any
    session_id: str | None
    bypass_cache: bool
    disable_cache: bool
    no_cache_read: bool
    no_cache_write: bool
    shared_data: dict[str, Any] | None
    wait_until: str
    page_timeout: int
    wait_for: str | None
    wait_for_images: bool
    delay_before_return_html: float
    mean_delay: float
    max_range: float
    semaphore_count: int
    js_code: Any
    js_only: bool
    ignore_body_visibility: bool
    scan_full_page: bool
    scroll_delay: float
    process_iframes: bool
    remove_overlay_elements: bool
    simulate_user: bool
    override_navigator: bool
    magic: bool
    adjust_viewport_to_content: bool
    screenshot: bool
    screenshot_wait_for: float | None
    screenshot_height_threshold: int
    pdf: bool
    capture_mhtml: bool
    image_description_min_word_threshold: int
    image_score_threshold: int
    table_score_threshold: int
    exclude_external_images: bool
    exclude_all_images: bool
    exclude_social_media_domains: list[str] | None
    exclude_external_links: bool
    exclude_social_media_links: bool
    exclude_domains: list[str] | None
    exclude_internal_links: bool
    verbose: bool
    log_console: bool
    capture_network_requests: bool
    capture_console_messages: bool
    method: str
    stream: bool
    url: str | None
    check_robots_txt: bool
    user_agent: str | None
    user_agent_mode: str | None
    user_agent_generator_config: dict[str, Any]
    deep_crawl_strategy: Any
    experimental: dict[str, Any] | None


class SpiderBase[Item: BaseItem](
    AutoItemModel[Item], PipelineHelper, ExtraContextMixin
):
    data_dir: str = "default"
    custom_browser_config: BrowserConfigDict = {}  # noqa: RUF012
    custom_run_config: CrawlerRunConfigDict = {}  # noqa: RUF012
    verbose: bool = True
    ignore_html: bool = False
    ignore_markdown: bool = False
    limit: int = 0
    concurrent_requests: int = 1

    def __init__(self, batch_id: str | None = None, **kwargs: Any) -> None:
        self.batch_id = batch_id
        self.update_extra_context(**kwargs)

    async def start(self) -> None:
        pass

    async def close(self) -> None:
        pass

    def get_browser_config(self, **kwargs: Unpack[BrowserConfigDict]) -> Any:
        raise NotImplementedError("Must implement method crawl.")

    def get_run_config(
        self, item: Item, **kwargs: Unpack[CrawlerRunConfigDict]
    ) -> dict[str, Any]:
        run_config: dict[str, Any] = {
            "page_timeout": 120000,
            "wait_for": """js:() => {
                const cloudflareLink = document.querySelector('a[href*="cloudflare.com"]');
                const body = document.querySelector('body');
                return !cloudflareLink && !!body && body.innerHTML.trim() !== '';
            }""",
            "keep_data_attributes": True,
            "remove_forms": True,
            "word_count_threshold": 0,
            **self.custom_run_config,
            **kwargs,
        }

        return run_config

    def check_result(self, item: Item, result: Any) -> None:
        if not result or not result.cleaned_html:
            print(f"Result is None for {item.url} in {self.__class__.__name__}")
            raise ResultIsEmpty(f"Result is None for {item.url}")
        elif result.cleaned_html:
            soup = BeautifulSoup(result.cleaned_html, "lxml")
            if soup.select_one('a[href*="cloudflare.com"]'):
                result.success = False
            if result.status_code and result.status_code >= 400:
                error = (
                    f"Received non-200 status code {result.status_code} for {item.url}"
                )
                print(error)
                result.success = False

    def get_url(self, item: Item) -> str:
        return item.url

    async def crawl(self, item: Item) -> Any:
        raise NotImplementedError("Must implement method crawl.")

    async def run(self, item: Item) -> tuple[Any, Any]:
        with TimeStatistic() as time_stats:
            crawl_result = await self.crawl(item)

        res = cast(Any, crawl_result)
        if isinstance(crawl_result, list):
            res = crawl_result[0]
        elif isinstance(crawl_result, AsyncGenerator):
            async for r in crawl_result:
                res = r
                break

        result = self.get_result(item, res)
        await self.save_result(result)
        stats = self.get_stats(item, res, time_stats)
        await self.log_stats(stats)
        return result, stats

    def get_result(
        self,
        item: Item,
        result: Any,
    ) -> dict[str, Any] | None:
        self.check_result(item, result)
        return self.transform_result(item, result)

    def transform_result(
        self,
        item: Item,
        result: Any,
    ) -> dict[str, Any] | None:
        result.url = item.url
        if self.ignore_html:
            result.html = result.html[:1000]
        if self.ignore_markdown:
            result.markdown = None
        return cast("dict[str, Any]", result.model_dump())

    def get_stats(self, item: Item, result: Any, time_stats: TimeStatistic) -> dict[str, Any]:
        return {
            "url": item.url,
            "spider_name": self.__class__.__name__,
            "collection": self.collection_name,
            "html_bytes": len(result.html.encode("utf-8")),
            "cleaned_html_bytes": (
                len(result.cleaned_html.encode("utf-8")) if result.cleaned_html else 0
            ),
            "status_code": result.status_code,
            **time_stats.to_dict(),
        }


class DockerSpider[Item: BaseItem](SpiderBase[Item]):
    base_url: str | None = None

    def get_browser_config(self, **kwargs: Unpack[BrowserConfigDict]) -> Any:
        from crawl4ai import BrowserConfig

        browser_config: dict[str, Any] = {
            "headless": True,
            "verbose": True,
            "browser_type": "undetected",
            **self.custom_browser_config,
            **kwargs,
        }
        return BrowserConfig(**browser_config)

    async def crawl(self, item: Item) -> Any:
        from crawl4ai import Crawl4aiDockerClient, CrawlerRunConfig

        if self.base_url is None:
            return

        async with Crawl4aiDockerClient(base_url=self.base_url) as client:
            # Configure crawl
            await client.authenticate("hartefang@gmail.com")
            browser_config = self.get_browser_config()
            crawler_config = self.get_run_config(item)

            result = await client.crawl(
                [self.get_url(item)],
                browser_config=browser_config,
                crawler_config=CrawlerRunConfig(**crawler_config),
            )

            return result


class BrowserSpider[Item: BaseItem](SpiderBase[Item]):
    builtin_browser: bool = False

    @cached_property
    def crawler(self) -> Any:
        from crawl4ai import AsyncWebCrawler

        return AsyncWebCrawler(config=self.get_browser_config(), verbose=self.verbose)

    async def start(self) -> None:
        await self.crawler.start()

    async def close(self) -> None:
        await self.crawler.close()

    def get_browser_config(self, **kwargs: Unpack[BrowserConfigDict]) -> Any:
        from crawl4ai import BrowserConfig

        if self.builtin_browser:
            browser_config: dict[str, Any] = {
                "browser_mode": "builtin",
                # "browser_mode": "dedicated",
                "headless": False,
                "use_persistent_context": True,
                "user_data_dir": f"./.crawl4ai/profiles/{self.data_dir}",
                "viewport_width": 1920,
                "viewport_height": 1080,
                "text_mode": True,
                **self.custom_browser_config,
                **kwargs,
            }
        else:
            browser_config = {
                "headless": True,
                "verbose": True,
                "text_mode": True,
                **self.custom_browser_config,
                **kwargs,
            }
        return BrowserConfig(**browser_config)

    async def crawl(self, item: Item) -> Any:
        from crawl4ai import CrawlerRunConfig, CrawlResultContainer

        crawl_result = await self.crawler.arun(
            url=self.get_url(item),
            config=CrawlerRunConfig(**self.get_run_config(item)),
        )
        if isinstance(crawl_result, CrawlResultContainer):
            return crawl_result._results
        else:
            return crawl_result


class HTTPSpider[Item: BaseItem](
    AutoItemModel[Item], PipelineHelper, ExtraContextMixin
):
    method: str = "GET"
    json_response: bool = True
    base_url: str = ""
    params: dict[str, str] | None = None
    headers: dict[str, str] | None = None
    cookies: dict[str, str] | None = None
    limit: int = 0
    concurrent_requests: int = 1

    def __init__(self, batch_id: str | None = None, **kwargs: Any) -> None:
        self.batch_id = batch_id
        self.update_extra_context(**kwargs)

    async def start(self) -> None:
        pass

    async def close(self) -> None:
        pass

    def check_result(self, result: Response) -> None:
        result.raise_for_status()

    def get_url(self, item: Item) -> str:
        return self.base_url or item.url

    def get_params(self, item: Item) -> dict[str, Any] | None:
        return self.params

    def get_headers(self, item: Item) -> dict[str, Any] | None:
        return self.headers

    def get_cookies(self, item: Item) -> dict[str, Any] | None:
        return self.cookies

    def get_json(self, item: Item) -> dict[str, Any] | None:
        return None

    async def run(self, item: Item) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        url = self.get_url(item)
        params = self.get_params(item)
        headers = self.get_headers(item)
        cookies = self.get_cookies(item)
        if not url:
            raise ValueError("Either url or base_url must be provided.")

        with TimeStatistic() as time_stats:
            async with AsyncClient() as client:
                if self.method.upper() == "POST":
                    json = self.get_json(item)
                    response = await client.post(
                        url,
                        headers=headers,
                        cookies=cookies,
                        params=params,
                        json=json,
                    )
                else:
                    response = await client.get(
                        url,
                        headers=headers,
                        cookies=cookies,
                        params=params,
                    )

        result = self.get_result(response)
        await self.save_result(result)
        stats = self.get_stats(response, time_stats)
        await self.log_stats(stats)
        return result, stats

    def get_result(
        self,
        response: Response,
    ) -> dict[str, Any] | None:
        self.check_result(response)
        return self.transform_result(response)

    def transform_result(
        self,
        response: Response,
    ) -> dict[str, Any] | None:
        if self.json_response:
            if response.text is None:
                print(f"API request returned empty response for {response.url}")
                return {}
            try:
                return cast(dict[str, Any], response.json())
            except Exception as e:
                print(f"Failed to decode JSON response for {response.url}: {e}")
                return None
        else:
            return {"raw_text": response.text}

    def get_stats(self, response: Response, time_stats: TimeStatistic) -> dict[str, Any]:
        return {
            "url": str(response.url),
            "spider_name": self.__class__.__name__,
            "collection_name": self.collection_name,
            "response_bytes": len(response.text.encode("utf-8")),
            "status_code": response.status_code,
            **time_stats.to_dict(),
        }


class CDPSpider:
    def __init__(self, cdp_url: str) -> None:
        self.cdp_url = cdp_url

    async def inspect_request(
        self, target_url: str, request_keyword: str, timeout: int = 30000
    ) -> dict[str, Any] | None:
        # 1. 启动异步 Playwright 上下文
        from playwright.async_api import async_playwright

        async with async_playwright() as p:
            # 异步连接到 CDP
            # url = f"http://{self.cdp_address}:{self.cdp_port}"
            browser = await p.chromium.connect_over_cdp(self.cdp_url)

            # 获取上下文和页面 (获取属性是同步的，但 new_page() 是异步的)
            context = browser.contexts[0]
            if context.pages:
                page = context.pages[0]
            else:
                page = await context.new_page()

            tries = 0
            result: dict[str, Any] | None = None
            print(f"准备打开网页并等待包含 '{request_keyword}' 的请求...")

            while tries < 3:
                try:
                    # 2. 异步监听请求
                    # 注意：在异步版本中，expect_request 是一个异步上下文管理器
                    async with page.expect_request(
                        lambda request: request_keyword in request.url, timeout=timeout
                    ) as request_info:
                        # 在上下文管理器内部触发页面跳转
                        await page.goto(target_url)

                    # 3. 等待并解析捕获到的请求对象
                    target_request = await request_info.value

                    # ====== 4. 提取所需的数据 ======

                    # 获取 URL (属性，直接读取)
                    parsed = urlparse(target_request.url)
                    params = dict(parse_qsl(parsed.query))

                    # 获取 Headers (异步方法 all_headers() 更可靠，返回字典，key 自动转为小写)
                    all_headers = await target_request.all_headers()
                    headers = {
                        k: v
                        for k, v in all_headers.items()
                        if not k.startswith(":") and k != "cookie"
                    }

                    # 获取 Cookies (从 header 的 cookie 字段中提取)
                    cookies = None
                    if "cookie" in all_headers:
                        cookies = dict(
                            [
                                p.split("=", maxsplit=1)
                                for p in all_headers["cookie"].split("; ")
                            ]
                        )

                    result = {"params": params, "headers": headers, "cookies": cookies}
                    break
                except Exception as e:
                    print(f"超时或发生错误: {e}")
                    tries += 1

            # 5. 断开连接
            await browser.close()

            return result
