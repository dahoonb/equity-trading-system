import time
import threading
from ibapi.wrapper import EWrapper
from ibapi.client import EClient

class TestApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.connected_event = threading.Event()

    def nextValidId(self, orderId):
        super().nextValidId(orderId)
        print(f"Successfully connected! nextValidId: {orderId}")
        self.connected_event.set() # Signal success

    def error(self, reqId, errorCode, errorString):
        super().error(reqId, errorCode, errorString)
        print(f"Error: {errorCode} - {errorString}")
        # If connection error, signal failure
        if errorCode in [504, 1100, 1101, 1102, 507, 509]:
             self.connected_event.set() # Allow wait to finish on error too

    def connectionClosed(self):
        super().connectionClosed()
        print("Connection closed.")
        self.connected_event.set() # Allow wait to finish


app = TestApp()
host = '127.0.0.1'
port = 7497 # Make sure this matches TWS/Gateway Port
clientId = 5 # Use a different client ID for testing

print(f"Attempting to connect to {host}:{port} with ClientID {clientId}...")
app.connect(host, port, clientId)

# Start client thread
thread = threading.Thread(target=app.run, daemon=True)
thread.start()

# Wait for connection result (success or error)
print("Waiting for connection confirmation (nextValidId or error)...")
connected = app.connected_event.wait(timeout=15) # Increased timeout slightly

if not connected and not app.isConnected():
     print("Connection attempt timed out or failed.")
elif app.isConnected():
     print("Connection seems successful (nextValidId likely received).")
     # Add a short delay to allow messages before disconnect
     time.sleep(2)
else:
     print("Connection failed based on error callbacks.")


print("Disconnecting...")
app.disconnect()
thread.join(timeout=5)
print("Finished.")