import pytest
import datetime
from punto2 import procesar_archivos, BUCKET_NAME_ORIGEN, BUCKET_NAME_DESTINO
import boto3
from bs4 import BeautifulSoup
from unittest.mock import MagicMock
import codecs  # Para manejar el BOM

# HTML de prueba
HTML_TEST = """
<html>
<body>
    <div class="listings">
        <a class="listing listing-card">
            <div class="listing-card__location__geo">Bogotá, Centro</div>
            <span class="price__actual">$300,000,000</span>
            <p data-test="bedrooms">3</p>
            <p data-test="bathrooms">2</p>
            <p data-test="floor-area">80 m²</p>
        </a>
    </div>
</body>
</html>
"""

@pytest.fixture
def s3_mock():
    s3 = MagicMock()
    yield s3

def test_procesar_archivos(s3_mock, monkeypatch):
    """Verifica que procesar_archivos extrae datos y sube el CSV correctamente."""
    
    # Mock de boto3
    monkeypatch.setattr("punto2.s3_client", s3_mock)
    monkeypatch.setattr("punto2.BUCKET_NAME_ORIGEN", "parcialconexion")
    monkeypatch.setattr("punto2.BUCKET_NAME_DESTINO", "casasprueba")
    
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    filename = f"casas_{today}.csv"
    
    # Simulación de respuesta de S3 con archivos HTML
    s3_mock.list_objects_v2.return_value = {
        "Contents": [{"Key": f"landing-casas-{today.replace('-', '')}/test.html"}]
    }
    
    # Simulación de la respuesta de get_object para un archivo HTML
    s3_mock.get_object.return_value = {"Body": MagicMock(read=lambda: HTML_TEST.encode("utf-8"))}
    
    resultado = procesar_archivos()
    print("Resultado:", resultado)
    print("Llamadas al mock:", s3_mock.mock_calls)
    
    # Verificar que se haya llamado correctamente el método put_object
    s3_mock.put_object.assert_called()
    
    # Verificar contenido del CSV generado
    args, kwargs = s3_mock.put_object.call_args
    contenido_csv = kwargs["Body"].decode("utf-8")

    # Eliminar BOM si existe
    contenido_csv = contenido_csv.lstrip(codecs.BOM_UTF8.decode("utf-8"))

    header = "FechaDescarga,Barrio,Valor,NumHabitaciones,NumBanos,mts2"
    expected_content = f"{header}\n{today},Bogotá, Centro,$300,000,000,3,2,80 m²"

    print("contenido real: ", contenido_csv.strip())
    print("contenido esperado: ", expected_content.strip())

    assert contenido_csv.strip() == expected_content.strip(), f"El contenido no coincide: {contenido_csv}"
    assert "Proceso completado" in resultado
