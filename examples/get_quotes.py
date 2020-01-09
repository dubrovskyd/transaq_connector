# -*- coding: utf-8 -*-
"""
Пример использования коннектора.
"""
from transaq.commands import *
import time

path = ""
if __file__ is not None:
    path = os.path.dirname(__file__)
    if path != "":
        path += os.sep

log_path = 'C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python38\\lib\\site-packages\\transaq\\Logs' #path + "Logs"


def handle_txml_message(msg):
    print(msg)


if __name__ == '__main__':
    try:
        reload_dll('C:\\Users\\user\\PycharmProjects\\transaq_connector\\txmlconnector64.dll')
        initialize(log_path, 3, handle_txml_message)
        print(connect("Логин", "Пароль", "78.41.194.46:3950"))
        time.sleep(3)
        print(get_history('TQBR', 'GAZP', 2, count=10))
        time.sleep(3)
    finally:
        print(disconnect())
        uninitialize()
