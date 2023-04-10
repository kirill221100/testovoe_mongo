from aiogram import Bot, Dispatcher, executor, types
from core.config import Config as cfg
from db.utils import aggregate
import json

bot = Bot(token=cfg.BOT_API)
dp = Dispatcher(bot)


@dp.message_handler()
async def echo(message: types.Message):
    try:
        mes = json.loads(message.text)
        res = await aggregate(mes['dt_from'], mes['dt_upto'], mes['group_type'])
        await message.answer(json.dumps(res))
    except json.decoder.JSONDecodeError:
        await message.answer('''Невалидный запос. Пример запроса:
{"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}''')
    except (KeyError, ValueError):
        await message.answer('''Допустимо отправлять только следующие запросы:
{"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}
{"dt_from": "2022-10-01T00:00:00", "dt_upto": "2022-11-30T23:59:00", "group_type": "day"}
{"dt_from": "2022-02-01T00:00:00", "dt_upto": "2022-02-02T00:00:00", "group_type": "hour"}''')

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
