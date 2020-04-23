from commands import *
import time

connector_ready = False

def handle_txml_message(msg):
    if isinstance(msg, ServerStatus):
        print('ServerStatus connected: %s text: %s' % (msg.connected, msg.text))
        if msg.connected == "true":
            global connector_ready
            connector_ready = True
            print('connected')
        else:
            print('not connected')
    elif isinstance(msg, TextMessagePacket):
        for itm in msg.items:
            print('Text Message: %s' % itm.text)
    elif isinstance(msg, SecInfoUpdate) or isinstance(msg, SecurityPitPacket) or isinstance(msg, SecurityPacket):
        if 1 > 2:
            print('ignore msg %s' % str(type(msg)))              
    else:
        print('msg received of type %s' % str(type(msg)))

# python -u change_pass.py default 0 FZTC00000A 12345678 87654321
if __name__ == '__main__':
    try:
        initialize("Logs", 2, handle_txml_message)
        server = sys.argv[1]
        port = sys.argv[2]
        if server == 'default':
            server = "78.41.199.12"
        if port == 'default' or port == '0':
            port = "3900"            
        terminal = sys.argv[3]
        curr_pwd = sys.argv[4]
        new_pwd = sys.argv[5]
        print ('connecting to %s:%s' % (server, port))
        conn = connect(terminal, curr_pwd, server + ":" + port)
        while not connector_ready:
            print('waiting %s ' % (str(connector_ready)))
            time.sleep(5)
        print('connected. changing password')
        cpass = change_pass(curr_pwd, new_pwd)
        print(cpass)
        time.sleep(10)
    finally:
        print('disconnecting')
        print(disconnect())
        uninitialize()
