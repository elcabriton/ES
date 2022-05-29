import tabula as tb
import pandas as pd
import PyPDF2
import math
from pymongo import MongoClient
cluster = "mongodb+srv://lucasd:lucasd@basededados.fau4o.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(cluster) 
db = client.app
collection = db.cursos
file = 'isabela.pdf'
pdfFileObj = open (file, 'rb')
pdfReader = PyPDF2.PdfFileReader (pdfFileObj)
pt = pdfReader.numPages
nomecurso = tb.read_pdf(file, area = (130, 80, 150, 300) ,columns =[], pages = '1',pandas_options={'header': None}, stream=True)[0]
#print(nomecurso)
#nome = tb.read_pdf(file, area = (110, 80, 130, 300) ,columns =[], pages = '1',pandas_options={'header': None}, stream=True)[0]
#print(nome)
for i in range(1, pt+1):
    if i > 1 :
        df = pd.concat([df, tb.read_pdf(file, pages = i, area = (250, 0, 820, 595),  columns = [70, 250, 300, 320, 370, 500], pandas_options={'header': None}, stream=True)[0]], ignore_index=True, axis=0)
    else :
        df = tb.read_pdf(file, pages = i, area = (250, 0, 820, 595),  columns = [70, 250, 300, 320, 370, 500], pandas_options={'header': None}, stream=True)[0]
#print(df)
df.rename(columns={ 0: 'Código', 1: 'Disciplina', 2: 'C.H.', 3: 'Cred.', 4: 'Situacao', 5: 'Periodo/Ano', 6: 'Periodo Ideal'}, inplace = True)
curso = []
semestre = []
horasc = []
hsemestres = []
for index, row in df.iterrows():
    if type(row['Disciplina']) != str :
        a1 = row['Código']
        a2 = row['C.H.']
        a3 = row['Cred.']
        a4 = row['Situacao']
        a5 = row['Periodo Ideal']
    if type(row['Situacao']) != str and type(row['Código']) == float:
        row['Código'] = a1
        row['C.H.'] = a2
        row['Cred.'] = a3
        row['Situacao'] = a4
        row['Periodo Ideal'] = a5
    if row['Código'].count("ACG") == 1 :
        horasc.append(row)
    else :
        if row['Código'].count("BA") == 0  :
            if row['Código'].count("Autentica") == 0 and row['Código'].count("Estrutura") == 0:
                if math.isnan(float(row[5])) == False : 
                    chs = {
                        'semestre' : row[0] + row[1],
                        'che' : int(row[5]),
                        'chv' : int(row[6])}
                    hsemestres.append(chs)
            if len(semestre) != 0 :
                #pprint.pprint(semestre)
                curso.append(semestre.copy())
                semestre.clear()
        else:
            if type(row['Disciplina']) == str :
                semestre.append(row)
                if index+1 == len(df.index) :
                    curso.append(semestre.copy())
                    semestre.clear()
cur = {
    'nome' : nomecurso[0].to_string(index=False),
    'cadeiras' : {
        'semestre' : [[]]  
    },
    'horas' : []
}
def myFunc(e):
    return float(e[0]['Periodo Ideal'])
curso.sort(key=myFunc)
for i in curso:
    b = 0
    for j in i :
        doc = {
            'Código' : j['Código'],
            'Disciplina' : j['Disciplina'],
            'C.H.' : j['C.H.'],
            'Cred.' : j['Cred.'],
            'Situacao' : j['Situacao'],
            'Periodo/Ano' : j['Periodo/Ano'],
            'Periodo Ideal' : j['Periodo Ideal']
        }
        if math.isnan(float(j['Periodo Ideal'])) == False :
            a = int(j['Periodo Ideal']) - 1
        else :
            a = 10
        if a > b:
            cur['cadeiras']['semestre'].append([])
        cur['cadeiras']['semestre'][a].append(doc)
        b = a
for i in horasc:
    doc = {
        'Código' : i['Código'],
        'Disciplina' : i['Disciplina'],
        'C.H.' : i['C.H.'],
        'Situacao' : i['Situacao'],
        'Periodo/Ano' : i['Periodo/Ano']
    }
    cur['horas'].append(doc)

tc = 0
t = 0
for i in hsemestres : 
        t = t + i['che']
        tc = tc + i['chv']
avanco = (tc*100)/t
print(avanco)
#collection.insert_one(cur)
#result = collection.find({})
#for i in result:
#    pprint.pprint(i)
