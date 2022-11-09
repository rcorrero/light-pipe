from typing import AsyncGenerator, Iterable, Optional

import aiohttp
from light_pipe import AsyncGatherer


class AiohttpGatherer(AsyncGatherer):
    def __init__(
        self, session: Optional[aiohttp.ClientSession] = None, 
        use_auth: Optional[bool] = True, login: Optional[str] = None, 
        password: Optional[str] = ""
    ):
        if session is None:
            if use_auth:
                session = self._make_session_with_auth(
                    login=login, password=password
                )
            else:
                session = aiohttp.ClientSession()
        self.session = session
        self.use_auth = use_auth
        self.login = login
        self.password = password


    def _make_session_with_auth(
        self, login: str, password: str,
    ):
        auth = aiohttp.BasicAuth(login, password)
        session = aiohttp.ClientSession(auth=auth)
        return session


    async def _async_gen(
        self, iterable: Iterable,
    ) -> AsyncGenerator:
        async with self.session as session:
            results = super()._async_gen(iterable=iterable, session=session)
            async for result in results:
                yield result
        await self.session.close()
