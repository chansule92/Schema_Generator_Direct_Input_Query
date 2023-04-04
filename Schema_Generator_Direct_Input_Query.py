import sys 
from PyQt5.QtWidgets import QApplication,  QPushButton, QLabel,QGridLayout,QLineEdit, QHBoxLayout,QVBoxLayout,QWidget,QRadioButton,QFileDialog, QTextEdit
from PyQt5.QtCore import Qt
import os
import cx_Oracle
import json
import pandas as pd
import pymysql
from pathlib import Path

class Exam(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()
    
    def initUI(self):
        #프로그램 형태 만들기
        grid = QGridLayout()
        grid.addWidget(QLabel("Database Type :"),0,0) 
        grid.addWidget(QLabel("Database Schema :"),1,0)
        grid.addWidget(QLabel("Database Name :"),2,0)
        grid.addWidget(QLabel("Host Address :"),3,0)
        grid.addWidget(QLabel("Port Number :"),4,0)
        grid.addWidget(QLabel("User :"),5,0)
        grid.addWidget(QLabel("Password :"),6,0)
        grid.addWidget(QLabel("Connect_file Name :"),7,0)
        grid.addWidget(QLabel("Schema Name :"),8,0)
        grid.addWidget(QLabel("SQL QUERY :"),9,0)
        grid.addWidget(QLabel("Path:"),10,0)
        self.PATH = QLabel(" ")
        grid.addWidget(self.PATH,10,1)
        #입력 변수 생성
        self.result = QLabel("Default")
        self.oracle = QRadioButton("oracle")
        self.mysql = QRadioButton("mysql")
        self.mariadb = QRadioButton("mariadb")
        self.SCHEMA = QLineEdit()
        self.DB_NAME = QLineEdit()
        self.HOST = QLineEdit()
        self.PORT = QLineEdit()
        self.USER = QLineEdit()
        self.PW = QLineEdit()
        self.PW.setEchoMode(QLineEdit.Password)
        self.CONN_FILE = QLineEdit()
        self.SCHEMA_NAME = QLineEdit()
        self.SQL_QUERY = QTextEdit()
        self.SEARCH_BUTTON = QPushButton("찾아보기...")
        self.SEARCH_BUTTON.clicked.connect(self.PATH_SELECT)
        #입력칸 배치
        typehbox = QHBoxLayout()
        typehbox.addWidget(self.oracle,0)
        typehbox.addWidget(self.mysql,1)
        typehbox.addWidget(self.mariadb,2)
        grid.addLayout(typehbox,0,1)
        grid.addWidget(self.SCHEMA,1,1)
        grid.addWidget(self.DB_NAME,2,1)
        grid.addWidget(self.HOST,3,1)
        grid.addWidget(self.PORT,4,1)
        grid.addWidget(self.USER,5,1)
        grid.addWidget(self.PW,6,1)
        grid.addWidget(self.CONN_FILE,7,1)
        grid.addWidget(self.SCHEMA_NAME,8,1)
        grid.addWidget(self.SQL_QUERY,9,1)
        grid.addWidget(self.SEARCH_BUTTON,10,2)

        #DB선택 라디오버튼
        self.oracle.toggled.connect(self.DBTYPE)
        self.mysql.toggled.connect(self.DBTYPE)
        self.mariadb.toggled.connect(self.DBTYPE)
        #생성,취소버튼
        CreateButton = QPushButton("생성")
        CancleButton = QPushButton("취소")
        CreateButton.clicked.connect(self.CreateJson)
        CancleButton.clicked.connect(self.close)

        hbox = QHBoxLayout()
        hbox.addStretch(1)
        hbox.addWidget(CreateButton)
        hbox.addWidget(CancleButton)


        vbox = QVBoxLayout()
        vbox.addLayout(grid)
        vbox.addWidget(self.result)
        vbox.addLayout(hbox)

        self.setLayout(vbox)

        
        self.setGeometry(400,400,400,300)
        self.setWindowTitle('JSON Maker')
        self.show()
    
    def PATH_SELECT(self):
        dirName = QFileDialog.getExistingDirectory(self,self.tr("Open Data file"),"./",QFileDialog.ShowDirsOnly)
        self.dirName = dirName.replace('''/''','''\\''')
        self.PATH.setText(dirName)
        return dirName
        
        #DB에 따라 입력칸 보이기 or 안보이기
    def DBTYPE(self):
        radioBtn = self.sender()
        if radioBtn.isChecked():
            self.DB_TYPE = radioBtn.text()
        if radioBtn.text() == 'mysql' or radioBtn.text()=='mariadb':
            self.SCHEMA.hide()
        elif radioBtn.text() == 'oracle':
            self.SCHEMA.show()
        #종료 함수
    def close(self):
        sys.exit()
        #생성 함수
    def CreateJson(self):
        try:
            database = self.DB_TYPE
            db = self.SCHEMA.text() #Schema Name에서 입력받은 값
            dbname = self.DB_NAME.text() #Database Name에서 입력받은 값
            host = self.HOST.text() #Host Address 에서 입력받은 값
            port = self.PORT.text() #Port number 에서 입력받은 값 (str형)
            user = self.USER.text() #User 에서 입력받은 값
            password = self.PW.text() #Password 에서 입력받은 값
            connect_file = self.CONN_FILE.text() #Connect File에서 입력받은 값
            dirName = self.dirName #'찾아보기'로 선택한 경로
            Schema = self.SCHEMA_NAME.text() #입력받은 생성할 스키마 이름
            SQL = self.SQL_QUERY.toPlainText() # 입력받은 SQL QUERY

            if database == 'oracle':
                LOCATION=str(Path.cwd())+"\instantclient-basic-windows.x64-21.7.0.0.0dbru\instantclient_21_7" #오라클 인스턴트클라이언트 위치 (실행파일과 같은 위치에 있어야 합니다.)
                os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"]
                dsn=cx_Oracle.makedsn(host,int(port),dbname)
                con=cx_Oracle.connect(user,password,dsn,encoding='UTF-8')
                cursor=con.cursor()
            elif database == 'mysql' or database == 'mariadb':
                conn = pymysql.connect(host=host,port=int(port),user=user, password=password,db=dbname,charset='utf8')
            else :
                return print('지원하지않는DB입니다')
            result=[]
            schema_info={"connectionid":"",
                        "schema_name":"",
                        "dbms_type":database,
                        "desc":"",
                        "nogroupby_enabled":True,
                        "transpose_enabled":False,
                        "fact":
                                { "id":"",
                                "name":"",
                                "alias":"",
                                "desc":"",
                                "from_clause":"",
                                "fields":""
                                }
                        }
            #쿼리결과를 DataFrame으로
            if database == 'mysql' or database == 'mariadb':
                data=pd.read_sql_query(SQL,conn)
            elif database == 'oracle':
                data=pd.read_sql_query(SQL,con)
            #칼럼의 타입에 따라 필드 타입 결정
            a=[]
            for i in data.columns:
                if i == 'CUST_ID':
                    column_type = 'char_only'
                null=0
                for j in range(0,len(data)):
                    if data[i][j] == '' or pd.isnull(data[i][j]):
                        null=null+1
                    elif type(data[i][j])==str:
                        column_type = 'char'
                    else :
                        column_type = 'num'
                if null == len(data):
                    column_type = 'null'
                if column_type == 'num':
                    column_category = '분석지표'
                    column_statistics=["SUM("+i+")"]
                    filter_query = "SELECT 1 AS MINVALUE, 10000000 AS MAXVALUE"
                elif column_type=="char_only":
                    column_category="고객번호"
                    column_statistics=["COUNT("+i+")"]
                    filter_query = "SELECT "+i+" FROM ("+SQL+") M GROUP BY "+ i
                else:
                    column_category="분석관점"
                    column_statistics=["COUNT("+i+")"]
                    filter_query = "SELECT "+i+" FROM ("+SQL+") M GROUP BY "+ i
                #JSON 필드 생성    
                if column_type == 'null':
                    pass
                else :
                    a.append(
                        (i,
                        { "name":i,
                        "type":column_type,
                        "alias": i,
                        "desc":i,
                        "show":True,
                        "filter_query": filter_query,
                        "category":column_category,
                        "statistics":column_statistics,
                        "statistics_desc":i,
                        "source":"table"
                        } 
                        )
                        )
                #JSON 구성정보 생성
            schema_info["connectionid"] = connect_file
            schema_info["schema_name"] = Schema
            schema_info["desc"] = Schema
            schema_info["fact"]["id"] = Schema
            schema_info["fact"]["name"] = "("+SQL+")"
            schema_info["fact"]["alias"]= "M"
            schema_info["fact"]["desc"]= Schema
            schema_info["fact"]["fields"]=dict(a)
            print(schema_info)

            result.append(schema_info)
            #txt파일 생성
            with open(dirName + """\\{}.txt""".format(Schema),'w',encoding="UTF-8") as outfile:
                json.dump(schema_info,outfile,indent=4,ensure_ascii=False)
            #접속종료
            if database == 'oracle':
                cursor.close()
            elif database == 'mysql' or 'mariadb':
                conn.close()
        #오류처리
        except AttributeError :
            return self.result.setText("JSON파일 저장 경로가 지정 되지 않았거나 잘못된 값이 입력되었습니다")
        except pymysql.err.OperationalError:
            return self.result.setText("데이터베이스에 연결할 수 없습니다")
        except cx_Oracle.DatabaseError:
            return self.result.setText("데이터베이스에 연결할 수 없습니다")
        except ValueError:
            return  self.result.setText("잘못된 값이 입력되었습니다.")
        return self.result.setText('JSON파일이 생성 되었습니다')



    def tglStat(self,state):
        if state:
            self.statusBar().show()
        else:
            self.statusBar().hide()

    def keyPressEvent(self, e) :
        if e.key() == Qt.Key_Escape:
            self.close()

if __name__ == '__main__':
    app = QApplication(sys.argv)
    w = Exam()
    sys.exit(app.exec_())
