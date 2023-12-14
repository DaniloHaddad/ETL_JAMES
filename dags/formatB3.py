from src.repository import fileRepository as fr
from src.repository import marketTypeRepository as mtr
from src.repository import bdiRepository as bdir
from src.repository import priceCorrectionRepository as pcr
from src.repository import companyRepository as cr
from src.repository import paperRepository as pr
from src.repository import pregaoRepository as pregr
from src.service import fileService as fs

import os
import pendulum
from airflow.decorators import dag, task

@dag(
    schedule='0 */6 * * * ',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["pievi"],
)
def finalFunction():   
    @task
    def getFiles():
        anos = ['02012023','03012023', '08122023']
        files_path = []
        for ano in anos:
            name_file = f'COTAHIST_D{ano}.TXT'
            file_path = os.path.join('/opt/airflow/dags/src/data', name_file)
            files_path.append(file_path)
        return files_path

    @task
    def createStage(files_path):
        conn = fr.connectBdd()
        fr.drop_table(conn)
        fr.create_table(conn)

        for file_path in files_path:
            formattedFile = fs.fommaterb3(file_path)
            engine = fr.create_enginer()
            formattedFile.to_sql('btres', engine, if_exists='append', index=False)

        conn.commit()  
        conn.close()   

    @task
    def createStarSchema(files_path):
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

    files_path = getFiles()
    stage = createStage(files_path)
    starSchema = createStarSchema()

    files_path >> stage >> starSchema


finalFunction()
