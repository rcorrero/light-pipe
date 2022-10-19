from typing import AsyncGenerator, Callable, Iterable, Optional

import aiohttp
from light_pipe import AsyncGatherer


class AiohttpGatherer(AsyncGatherer):
    @classmethod
    def _make_session_with_auth(
        cls, login: str, password: str = "", *args, **kwargs
    ):
        auth = aiohttp.BasicAuth(login, password)
        session = aiohttp.ClientSession(auth=auth)
        return session


    @classmethod
    async def _fork(
        cls, f: Callable, iterable: Iterable, *args, 
        recurse: Optional[bool] = True, 
        session: Optional[aiohttp.ClientSession] = None, 
        use_auth: Optional[bool] = True, **kwargs
    ) -> AsyncGenerator:
        if session is None:
            if use_auth:
                session = cls._make_session_with_auth(*args, **kwargs)
            else:
                session = aiohttp.ClientSession()
        async with session as session:
            results = super()._fork(
                f, iterable, *args, recurse=recurse, session=session, 
                use_auth=use_auth, **kwargs
            )
            async for result in results:
                yield result
        await session.close()
