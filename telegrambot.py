import urllib
import urllib2

# Generate a bot ID here: https://core.telegram.org/bots#botfather
bot_id = "{Iotmicrophonebot}"

# Request latest messages
result = urllib2.urlopen("https://api.telegram.org/bot" + bot_id + "/getUpdates").read()
print (result)

# Send a message to a chat room (chat room ID retrieved from getUpdates)
result = urllib2.urlopen("https://api.telegram.org/bot" + bot_id + "/sendMessage", urllib.urlencode({ "chat_id": 0, "text": 'my message' })).read()
print (result)
