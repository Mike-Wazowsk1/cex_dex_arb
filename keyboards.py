import time
from telegram import ReplyKeyboardMarkup, Update, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup, \
    InputMediaPhoto, InputMedia, ReplyKeyboardRemove, InlineQueryResultArticle, InputTextMessageContent
class Keyboard:
    def __init__(self):
        self.main_keyboard = self.make_main_keyboard()
    
    def make_main_keyboard(self):
        keyboard = [[KeyboardButton(text='Список спредов'), KeyboardButton(text='Мониторинг')], [KeyboardButton(text='Объем (min)'),
                    KeyboardButton(text='Объем (max)')], [KeyboardButton(text='Биржи')]]
        rep = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        return rep