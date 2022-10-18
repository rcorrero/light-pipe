import asyncio
from typing import Callable, Generator, Iterable, Iterator, Optional

import aiohttp
from light_pipe import AsyncGatherer, Data


class AiohttpGatherer(AsyncGatherer):
    @classmethod
    def make_session_with_auth(cls, login: str, password: str = "", *args, **kwargs):
        auth = aiohttp.BasicAuth(login, password)
        session = aiohttp.ClientSession(auth=auth)
        return session


    @classmethod
    async def _fork(
        cls, f: Callable, iterable: Iterable, *args, 
        recurse: Optional[bool] = True, 
        session: Optional[aiohttp.ClientSession] = None, 
        use_auth: Optional[bool] = True, **kwargs
    ) -> Generator:
        if session is None:
            if use_auth:
                session = cls.make_session_with_auth(*args, **kwargs)
            else:
                session = aiohttp.ClientSession()
        async with session as session:
            tasks = list()
            for item in iterable:
                if recurse and (isinstance(item, Data) or isinstance(item, Iterator)):
                    tasks.append(cls._fork(
                        f, item, *args, recurse=recurse, session=session, **kwargs
                        )
                    )
                else:
                    tasks.append(f(item, *args, session=session, **kwargs))
            results = await asyncio.gather(
                *tasks
            )
        await session.close()
        return results   
