import sys 
from PyQt5.QtWidgets import QApplication,  QPushButton, QLabel,QGridLayout,QLineEdit, QHBoxLayout,QVBoxLayout,QWidget,QRadioButton,QFileDialog, QTextEdit
from PyQt5.QtCore import Qt
import os
import json
import pandas as pd
from pathlib import Path

class Exam(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()
    
    def initUI(self):
        #프로그램 형태 만들기
        grid = QGridLayout()
        grid.addWidget(QLabel("Database Type :"),0,0) 
        grid.addWidget(QLabel("Connect_file Name :"),1,0)
        grid.addWidget(QLabel("Schema Name :"),2,0)
        grid.addWidget(QLabel("SQL QUERY :"),3,0)
        grid.addWidget(QLabel("Path:"),4,0)
        self.PATH = QLabel(" ")
        grid.addWidget(self.PATH,4,1)
        #입력 변수 생성
        self.result = QLabel("Default")
        self.oracle = QRadioButton("oracle")
        self.mysql = QRadioButton("mysql")
        self.mariadb = QRadioButton("mariadb")
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
        grid.addWidget(self.CONN_FILE,1,1)
        grid.addWidget(self.SCHEMA_NAME,2,1)
        grid.addWidget(self.SQL_QUERY,3,1)
        grid.addWidget(self.SEARCH_BUTTON,4,2)

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
        self.setWindowTitle('Schema Maker')
        self.show()
    
    def PATH_SELECT(self):
        dirName = QFileDialog.getExistingDirectory(self,self.tr("Open Data file"),"./",QFileDialog.ShowDirsOnly)
        self.dirName = dirName.replace('''/''','''\\''')
        self.PATH.setText(dirName)
        return dirName
    
    def DBTYPE(self):
        radioBtn = self.sender()
        if radioBtn.isChecked():
            self.DB_TYPE = radioBtn.text()

        #종료 함수
    def close(self):
        sys.exit()
        #생성 함수
    def CreateJson(self):
        database = self.DB_TYPE
        connect_file = self.CONN_FILE.text() #Connect File에서 입력받은 값
        dirName = self.dirName #'찾아보기'로 선택한 경로
        Schema = self.SCHEMA_NAME.text() #입력받은 생성할 스키마 이름
        query = self.SQL_QUERY.toPlainText() # 입력받은 SQL QUERY

        result=[]
        schema_info={"connectionid":'quadmax',
                    "schema_name":'Schema',
                    "dbms_type":'database',
                    "desc":"테스트",
                    "nogroupby_enabled":True,
                    "transpose_enabled":False,
                    "remote_segments_import":True,
                    "fact":
                            { "id":"",
                            "name":"",
                            "alias":"",
                            "desc":"",
                            "from_clause":"",
                            "fields":""
                            }
                    }
        analytics_name = 'analytics_name'
        import sqlparse

        parsed = sqlparse.parse(query)
        query_result=[]
        # 파싱된 결과를 출력
        for statement in parsed:
            for token in statement.tokens:
                query_result.append(str(token.value).split('\n'))

        SELECT_LIST=[]
        FROM_LIST=[]
        GROUP_LIST=[]
        SELECT_YN = 0
        FROM_YN = 0
        GROUP_YN = 0
        for i in query_result:
            if i ==['SELECT']:
                SELECT_YN = 1
                FROM_YN = 0
                GROUP_YN = 0
            elif i ==['FROM']:
                SELECT_YN = 0
                FROM_YN = 1
                GROUP_YN = 0
            elif i ==['GROUP BY']:
                SELECT_YN = 0
                FROM_YN = 0
                GROUP_YN = 1
            if SELECT_YN == 1:
                SELECT_LIST.append(i)
            elif FROM_YN == 1:
                FROM_LIST.append(i)
            elif GROUP_YN == 1:
                GROUP_LIST.append(i)
        dimension_list={}
        for select in SELECT_LIST[2]:
            temp=(select.replace(' ',''))
            if temp[0] == ',':
                temp=temp[1:]
            dimension_list.update({temp.split('AS')[0]:temp.split('AS')[1]})

        dimension_list

        temp_from=''
        for k in FROM_LIST[2]:
            temp_from=temp_from + k
        from_list=temp_from[:-1]
        from_alias=temp_from[-1:]
        from_list
        for i in range(0,10):
            from_list=from_list.replace('  ',' ')
        abc=from_list.split(' ')
        if 'FROM' in abc:
            print()
        table_list=[]
        for i in range(0,len(abc)):
            if abc[i] == 'FROM' or abc[i] == 'JOIN':
                table_list.append([abc[i+1],abc[i+2]])

        schema_info["desc"]=analytics_name
        schema_info["fact"]["id"]=analytics_name
        schema_info["fact"]["name"]=from_list
        schema_info["fact"]["alias"]=query_alias
        schema_info["fact"]["desc"]=analytics_name

        column_table_mapping=[]
        for j in from_list.split(' '):
            word=j.split('.')
            if len(word)==2:
                for k in range(0,len(table_list)):
                    if word[0]==table_list[k][1]:
                        column_table_mapping.append([word[1],table_list[k][0]])
        MEASURE_LIST=list(dimension_list.keys())
        for i in MEASURE_LIST:
            column_name = ''
            column_type = ''
            column_desc = ''
            column_category = ''
            column_filter = ''
            column_statistics = ''
            if i in DIMENSION_LIST:
                column_type='char'
                column_category = '분석관점'
                column_statistics = "COUNT("+i+")"
                column_name = i.split('.')[-1]
            else:
                column_type='num'
                column_category = '분석지표'
                column_statistics = i
                column_filter = "SELECT 1 AS MINVALUE , 1000000000 as MAXVALUE FROM DUAL"
                column_name = i
            for j in result1:
                if i==j[0] and  len(j)==2:
                    column_desc=j[1]
            for k in column_table_mapping:
                if column_name == k[0] and column_filter == '':
                    column_filter = "SELECT "+k[0]+" FROM "+k[1]+ " GROUP BY "+k[0]
            a.append((column_name,
                    { "name":column_name,
                        "type":column_type,
                        "alias":column_name,
                        "desc":column_desc,
                        "show":True,
                        "filter_query":column_filter,
                        "category":column_category,
                        "statistics":column_statistics,                        
                        "statistics_desc":column_desc,
                        "source":"table"
                        }
                    )
                    )
        schema_info["fact"]["fields"]=dict(a)
        result.append(schema_info)
        #txt파일 생성
        with open(dirName + """\\{}.txt""".format(Schema),'w',encoding="UTF-8") as outfile:
            json.dump(schema_info,outfile,indent=4,ensure_ascii=False)




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
