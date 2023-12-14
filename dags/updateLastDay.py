from src.repository import fileRepository as fr
from src.repository import marketTypeRepository as mtr
from src.repository import bdiRepository as bdir
from src.repository import priceCorrectionRepository as pcr
from src.repository import companyRepository as cr
from src.repository import paperRepository as pr
from src.repository import pregaoRepository as pregr
from src.service import fileService as fs

from sqlalchemy import MetaData, Table, func
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import pytz
import os
import pendulum
from airflow.decorators import dag, task

@dag(
    schedule='0 */6 * * * ',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["pievi"],
)
def updateLastDay():
    @task
    def getFile():
        fuso_horario_brasil = pytz.timezone('America/Sao_Paulo')
        today = datetime.now(fuso_horario_brasil).date().strftime("%d%m%Y")
        print(today)
        
        name_file = f'COTAHIST_D{today}.TXT'
        file_path = os.path.join('/opt/airflow/dags/src/data', name_file)
        return file_path
    
    @task
    def updateStage(file_path):
        conn = fr.connectBdd()

        metadata = MetaData()
        btres = Table('btres', metadata, autoload=True, autoload_with=conn)
        
        Session = sessionmaker(bind=conn)
        session = Session()

        last_date = session.query(func.max(btres.c.data_pregao)).scalar()
        
        fuso_horario_brasil = pytz.timezone('America/Sao_Paulo')
        today = datetime.now(fuso_horario_brasil).date()

        if last_date and last_date < today:
            formattedFile = fs.fommaterb3(file_path)
            formattedFile.to_sql('btres', conn, if_exists='append', index=False)

        session.commit()  # Adicionado commit
        session.close()   # Adicionado close

    @task
    def updateStarSchema():
        conn = fr.connectBdd()

        mtr.create_table_market_type(conn)
        mtr.insert_table_market_type(conn)

        bdir.create_table_bdi(conn)
        bdir.insert_table_bdi(conn)
    
        pcr.create_table_price_correction(conn)
        pcr.insert_table_price_correction(conn)

        cr.create_table_company(conn)
        cr.insert_table_company(conn)

        pr.create_table_paper(conn)
        pr.insert_table_paper(conn)

        pregr.create_table_pregao(conn)
        pregr.insert_table_pregao(conn)

        pregr.create_relation(conn)

        conn.commit()  
        conn.close()  

    file_path = getFile()
    stage = updateStage(file_path)
    updateStarSchema = updateStarSchema()
    file_path >> stage >> updateStarSchema

updateLastDay()
