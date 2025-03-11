import boto3
import datetime
import json
import codecs
from bs4 import BeautifulSoup

# Configuración de S3
BUCKET_NAME_ORIGEN = "parcialconexion2"
BUCKET_NAME_DESTINO = "casasprueba2"
s3_client = boto3.client("s3")

def procesar_archivos():
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    folder_name = f"landing-casas-{today.replace('-', '')}"
    output_csv_path = f"casas_{today}.csv"
    propiedades = []

    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME_ORIGEN, Prefix=folder_name)
    
    if "Contents" not in response:
        print("⚠ No se encontraron archivos en el bucket de origen.")
        return
    
    archivos_html = [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".html")]

    for s3_key in archivos_html:
        print(f"📂 Procesando archivo: {s3_key}")
        
        try:
            response = s3_client.get_object(Bucket=BUCKET_NAME_ORIGEN, Key=s3_key)
            html_content = response['Body'].read().decode("utf-8")
            
            if not html_content.strip():
                print(f"⚠ El archivo {s3_key} está vacío o no contiene HTML válido.")
                continue

            soup = BeautifulSoup(html_content, "html.parser")
            listings_container = soup.find("div", {"class": lambda x: x and "listings" in x})
            
            if not listings_container:
                print(f"⚠ No se encontró el contenedor de propiedades en {s3_key}.")
                continue

            listings = listings_container.find_all("a", class_=lambda x: x and "listing" in x)
            print(f"✅ Se encontraron {len(listings)} propiedades en {s3_key}")

            for listing in listings:
                try:
                    barrio = listing.find("div", class_="listing-card__location__geo")
                    valor = listing.find("span", class_="price__actual")
                    num_habitaciones = listing.find("p", attrs={"data-test": lambda x: x and "bedrooms" in x})
                    num_banos = listing.find("p", attrs={"data-test": lambda x: x and "bathrooms" in x})
                    mts2 = listing.find("p", attrs={"data-test": lambda x: x and "floor-area" in x})

                    if not all([barrio, valor, num_habitaciones, num_banos, mts2]):
                        print(f"⚠ Se omitió una propiedad por falta de datos en {s3_key}")
                        continue

                    propiedad = [
                        today, 
                        barrio.text.strip(), 
                        valor.text.strip(), 
                        num_habitaciones.text.strip(), 
                        num_banos.text.strip(), 
                        mts2.text.strip()
                    ]
                    propiedades.append(propiedad)

                    print(f"✅ Propiedad extraída: {propiedad}")

                except Exception as e:
                    print(f"❌ Error procesando una propiedad en {s3_key}: {e}")
                    continue  

            # Extraer datos estructurados JSON-LD
            json_ld_script = soup.find("script", type="application/ld+json")
            if json_ld_script:
                try:
                    json_data = json.loads(json_ld_script.string)

                    # Verificar estructura de datos
                    if isinstance(json_data, dict) and "@graph" in json_data:
                        json_data = json_data["@graph"]

                    if isinstance(json_data, list):
                        for item in json_data:
                            if isinstance(item, dict) and item.get("@type") == "RealEstateListing":
                                barrio = item.get("address", {}).get("addressLocality", "N/A")
                                valor = item.get("offers", {}).get("price", "N/A")
                                num_habitaciones = item.get("numberOfRooms", "N/A")
                                num_banos = item.get("numberOfBathroomsTotal", "N/A")
                                mts2 = item.get("floorSize", {}).get("value", "N/A")
                                
                                propiedad = [
                                    today, barrio, valor, num_habitaciones, num_banos, mts2
                                ]
                                propiedades.append(propiedad)

                                print(f"✅ Propiedad extraída de JSON-LD: {propiedad}")

                except json.JSONDecodeError as e:
                    print(f"❌ Error al parsear JSON-LD en {s3_key}: {e}")
        
        except Exception as e:
            print(f"⚠ Error al obtener el archivo {s3_key}: {e}")

    # Guardar resultados en CSV en S3
    if propiedades:
        csv_content = "FechaDescarga,Barrio,Valor,NumHabitaciones,NumBanos,mts2\n"
        csv_content += "\n".join(",".join(map(str, row)) for row in propiedades)
        csv_bytes = codecs.BOM_UTF8 + csv_content.encode("utf-8")

        s3_client.put_object(
            Bucket=BUCKET_NAME_DESTINO,
            Key=output_csv_path,
            Body=csv_bytes,
            ContentType="text/csv"
        )
        print(f"📂 Archivo CSV guardado en {BUCKET_NAME_DESTINO}/{output_csv_path}")
    else:
        print("⚠ No se encontraron propiedades para guardar en CSV.")

    return f"Proceso completado. Archivo CSV en {BUCKET_NAME_DESTINO}/{output_csv_path}"

def lambda_handler(event, context):
    resultado = procesar_archivos()
    return {
        'statusCode': 200,
        'body': resultado
    }