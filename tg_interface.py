#type: ignore
from codecs import unicode_escape_decode, unicode_escape_encode
from email import message
# from captcha.image import ImageCaptcha
import random
import time
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    CallbackQueryHandler,
    ContextTypes,
    ConversationHandler,
    InlineQueryHandler
)
from telegram import ReplyKeyboardMarkup, Update, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup, \
    InputMediaPhoto, InputMedia, ReplyKeyboardRemove, InlineQueryResultArticle, InputTextMessageContent
import logging
from telegram.constants import ParseMode
import requests
import datetime

from database.db import DataBase
from keyboards import Keyboard
from uuid import uuid4
from arb import ArbitrageManager
API = "6191286786:AAHc2hvpRBkR0TSDS1dJmsUpPTegq5Wm_qE"

logging.basicConfig(format='%(levelname)s - %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)
KEYBOARD = Keyboard()
MIN_USDT = 10
MIN_AMOUNT = 0
arb = ArbitrageManager()
db = DataBase()

async def ave_me(update: Update, context:ContextTypes.DEFAULT_TYPE) -> None:

    await update.message.reply_text('Гений с большим членом: @god_cant_see_me')



async def start(update: Update, context:ContextTypes.DEFAULT_TYPE) -> None:

    await update.message.reply_text('CexDexArb Bot',reply_markup=KEYBOARD.main_keyboard)

async def monitoring(update: Update, context:ContextTypes.DEFAULT_TYPE):
    text = f"""
Установлено значение: {MIN_USDT}$
Если хотите изменить его,введите желаемую прибыль со сделки (в USDT).
Когда появится спред с прибылью не меньше указанной, я пришлю вам сообщение!
    (пример: 10.5)
Если хотите отключить поиск, напишите 0."""
    await update.message.reply_text(text,reply_markup=KEYBOARD.main_keyboard)


async def callback_handler(update: Update, context:ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if 'spread' in query.data:

        data = query.data.split("_")[1:]
        ex1,ex2,symbol = data
        value = 0
        asks_price1,bids_price1, asks_amount1,bids_amount1,timestamp1 = db.get_from_db(ex1,symbol)[0]
        asks_price2,bids_price2, asks_amount2,bids_amount2,timestamp2 = db.get_from_db(ex2,symbol)[0]
        text = f"""
{symbol}

\|[{ex1}](https)\| {str(round(asks_price1,6)).replace(".",",")} (15)
\|{ex2}\| {str(round(bids_price2,6)).replace(".",',')} (15)

Spread: {round(asks_price1*value -bids_price2*value)}"""
        await query.edit_message_text(text=text, parse_mode=ParseMode.MARKDOWN_V2)


async def spread_list(update: Update, context:ContextTypes.DEFAULT_TYPE):
    opps = arb.main()
    buttons  = []
    for i,op in enumerate(opps):
        symbol,ex1,ex2,bid,ask ,value = op
        if ex1 != 'gate' and ex2 !='gate':
            print(symbol,ex1,ex2,bid,ask ,value )
            buttons.append([InlineKeyboardButton(text = f"{symbol.upper()}: {round(ask*value)} -> {round(bid*value)}",callback_data=f'spread_{ex1}_{ex2}_{symbol}')])
    rep = InlineKeyboardMarkup(buttons,)
    await update.message.reply_text("Список спредов",reply_markup=rep)

async def volume_min(update: Update, context:ContextTypes.DEFAULT_TYPE):
    text = f"""
Установлен минимальный обьем: {MIN_AMOUNT}$
Если хотите изменить значение,введите желаемый объем (в USDT).
Все спреды будут появляться с объемами не менее указанного!
    (пример 100.5)
"""


def main() -> None:
    """Start the bot."""
    # Create the Application and pass it your bot's token.
    application = Application.builder().token(API).build()

    application.add_handler(CommandHandler('ave', ave_me))
    application.add_handler(CommandHandler('start', start))
    application.add_handler(MessageHandler(filters.Regex("Мониторинг"), monitoring))
    application.add_handler(MessageHandler(filters.Regex("Объем (min)"), volume_min))
    application.add_handler(MessageHandler(filters.Regex("Список спредов"), spread_list))
    application.add_handler(CallbackQueryHandler(callback_handler))






    application.run_polling()

if __name__ == "__main__":
    main()

