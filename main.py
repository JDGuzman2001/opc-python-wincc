# pip install OpenOPC-Python3x pywin32

import OpenOPC
import pywintypes
import time

pywintypes.datetime = pywintypes.TimeType

opc = OpenOPC.client()
opc.connect('OPC.SimaticNET')  # Exactly server name

while True:
    try:
        valor = opc['PLC_DB1.Presion_Tanque']  # Depends of the variable
        print("Presión del tanque:", valor)
        time.sleep(1)
    except Exception as e:
        print("Error leyendo variable:", e)
        break
