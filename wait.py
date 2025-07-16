import asyncio
import time
async def say_after(delay, what):
    print('Started')
    await asyncio.sleep(delay)
    print(what)
    return what




async def main():
    print(f"started at {time.strftime('%X')}")

    funcs = [say_after(5, 'hello'),
        say_after(2, 'world')] 
    res = await asyncio.gather(
        
        *funcs

    )
    print(f"finished at {time.strftime('%X')}")
    return res


print(asyncio.run(main()))
# say_after(5, "noice")
