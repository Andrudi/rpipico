from mqtt_as import MQTTClient
from mqtt_local import config
import uasyncio as asyncio
import dht, machine
import json
import network
import ubinascii
import settings
import os

# --- Pines ---
Pin_DHT22 = 15
Pin_Rele = 10

sensor_dht = dht.DHT22(machine.Pin(15))
pin_rele = machine.Pin(Pin_Rele, machine.Pin.OUT)
led_board = machine.Pin("LED", machine.Pin.OUT)

pin_rele.value(1) # Relé comienza apagado (es activo en bajo)
led_board.value(0) # LED apagado

#Valores por defecto
estado = {
    "setpoint": 25.0,
    "periodo": 10,       
    "modo": "auto",      
    "rele": 0            
}
temperatura_actual = 0.0
humedad_actual = 0.0


def guardar_configuracion():
    with open('config.json', 'w') as f:
        json.dump(estado, f)
    print("Configuración guardada en config.json")

def cargar_configuracion():
    global estado
    try:
        # Revisa si el archivo existe en la memoria de la placa
        if 'config.json' in os.listdir():
            with open('config.json', 'r') as f:
                config_data = json.load(f)
                estado['setpoint'] = config_data.get("setpoint", estado['setpoint'])
                estado['periodo'] = config_data.get("periodo", estado['periodo'])
                estado['modo'] = config_data.get("modo", estado['modo'])
                estado['rele'] = config_data.get("rele", estado['rele'])
            print(f"Configuración cargada desde config.json: {estado}")
        else:
            print("No se encontró config.json, usando valores por defecto.")
            guardar_configuracion() # Crea el archivo inicial
    except Exception as e:
        print("Error al cargar config.json:", e)

#Se carga el estado al inicial el programa
cargar_configuracion()

#Obtener ID del dispositivo a partir de la MAC
wlan = network.WLAN(network.STA_IF)
wlan.active(True)
mac_bytes = wlan.config('mac')
ID_Dispositivo = ubinascii.hexlify(mac_bytes).decode()

#Usado solamente en el simulador para probar
#ID_Dispositivo = "TermostatoPrueba"
     

#Función para hacer el destello
async def destello():
    for _ in range(15):
        led_board.value(1)
        await asyncio.sleep(0.2)
        led_board.value(0)
        await asyncio.sleep(0.2)

#Función para leer los mensajes 
async def leer_mensajes(client):
    async for topic, msg, retained in client.queue: 
        topico_completo = topic.decode()
        mensaje = msg.decode().strip()
        #Me quedo unicamente con el comando (setpoint, periodo, modo, rele o destello) para procesarlo
        comando = topico_completo.split('/')[-1] 
        print(f'Recibido -> Comando: {comando} | Mensaje: {mensaje}') 

        if comando == 'setpoint':
            try:
                estado["setpoint"] = float(mensaje)
                print(f"Setpoint actualizado a: {estado['setpoint']}")
                guardar_configuracion()
            except ValueError:
                print("Error: El setpoint debe ser un número.")
                
        elif comando == 'periodo':
            try:
                estado['periodo'] = int(mensaje)
                print(f"Periodo actualizado a: {estado['periodo']}")
                guardar_configuracion()
            except ValueError:
                print("Error: El periodo debe ser un entero.")
                
        elif comando == 'modo':
            if mensaje in ['auto', 'manual']:
                estado['modo'] = mensaje
                print(f"Modo cambiado a: {estado['modo']}")
                guardar_configuracion()
            else:
                print("Error: El modo solo puede ser 'auto' o 'manual'.")
                
        elif comando == 'rele':
            if mensaje == 'relé':
                if estado['modo'] == 'manual':
                    
                    if estado['rele'] == 0:
                        estado['rele'] = 1
                    else:
                        estado['rele'] = 0
                        
                    print(f"Relé alternado a: {estado['rele']}")
                    guardar_configuracion()
                else:
                    print("Ignorado: Se intentó mover el relé pero el modo es AUTO.")
                
        elif comando == 'destello':
            if mensaje == 'destello':
                print("Orden de destello recibida")
                asyncio.create_task(destello())
        

#Función para publicar los datos
async def publicar_datos(client):
    global temperatura_actual, humedad_actual
    while True:
        await asyncio.sleep(estado['periodo'])
        datos = {
            "temperatura": temperatura_actual,
            "humedad": humedad_actual,
            "setpoint": estado['setpoint'],
            "periodo": estado['periodo'],
            "modo": estado['modo']
        }
        datos_json = json.dumps(datos)
        print(f"Publicando: {datos_json}")
        try:
            await client.publish(ID_Dispositivo, datos_json, qos=1)
        except OSError:
            pass

#Funcion donde se subscrive a los topicos que son de interés
async def conexion(client):
    while True:
        await client.up.wait()
        client.up.clear()
        #Muestro esto para saber la MAC a la que me debo suscribir
        print(f"\n--- Se pudo conectar, el ID para suscribirse es: {ID_Dispositivo} ---\n")
        await client.subscribe(f"{ID_Dispositivo}/setpoint", 1)
        await client.subscribe(f"{ID_Dispositivo}/periodo", 1)
        await client.subscribe(f"{ID_Dispositivo}/destello", 1)
        await client.subscribe(f"{ID_Dispositivo}/modo", 1)
        await client.subscribe(f"{ID_Dispositivo}/rele", 1)

async def wifi_han(state):
    pass 


async def main(client):
    global temperatura_actual, humedad_actual
    await client.connect()

    #Metodo tomado de Peter Hinch para trabajar con queque
    for coroutine in (conexion, leer_mensajes, publicar_datos):
        asyncio.create_task(coroutine(client))
    
    while True:
        try:
            sensor_dht.measure()
            temperatura_actual = sensor_dht.temperature()
            humedad_actual = sensor_dht.humidity()
            
            if estado['modo'] == 'auto':
                if temperatura_actual >= estado['setpoint']:
                    pin_rele.value(0) # Enciende el relé
                    if estado['rele'] != 1:
                        estado['rele'] = 1
                        guardar_configuracion() # Guarda solo si cambió el estado del relé 
                else:
                    pin_rele.value(1) # Apaga el relé
                    if estado['rele'] != 0:
                        estado['rele'] = 0
                        guardar_configuracion()
        except OSError:
            print("Error de lectura en termostato")        
        
        if estado['modo'] == 'manual':
            if estado['rele'] == 1:
                pin_rele.value(0)
            else:
                pin_rele.value(1)

        await asyncio.sleep(2.5)


config['ssid'] = settings.SSID
config['wifi_pw'] = settings.password
config['server'] = settings.BROKER
config['wifi_coro'] = wifi_han
config['queue_len'] = 10
config['ssl'] = True 

MQTTClient.DEBUG = True
client = MQTTClient(config)

try:
    asyncio.run(main(client))
finally:
    client.close()
    asyncio.new_event_loop()