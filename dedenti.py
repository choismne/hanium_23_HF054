import easyocr
import cv2
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import glob
import re
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col
import boto3
import os
from PIL import Image


reader = easyocr.Reader(['en'])
spark = SparkSession.builder.appName("dedenti").getOrCreate()

def predict(test):  # test가 받아올 데이터, train가 학습시킬 기존 데이터

  date_pattern = r'(\d{2})\d{2}-\d{1,2}-\d{1,2}'
  test['date'] = test['extract'].str.extract(date_pattern)
  test['year'] = test['date']

  test.loc[test['date'].isnull(), 'year'] = test.loc[test['date'].isnull(), 'extract']

  test['extract']=test['year']
  test=test.drop(['date','year'],axis=1)

  test_data_spark = spark.createDataFrame(test, ['feature'])
  
  saved_model = PipelineModel.load('로컬경로/model_registry')
  prediction = saved_model.transform(test_data_spark)
  preds = prediction.filter(col("prediction") == 1).select("feature", "prediction")
  preds.show()
  #시간 환자번호 이름이 날아올거야
  preds = preds.toPandas()

  return preds

#s3에서 가져오는 코드
def read_image_from_s3(filename):
    bucket = s3.Bucket(bucket_name)
    object = bucket.Object(filename)
    response = object.get()
    file_stream = response['Body']
    img = Image.open(file_stream)
    img.save("로컬경로/before/{}".format(filename), 'jpeg')
    return img

s3 = boto3.resource('s3')
#여기서 가져올 버킷이름 설정
bucket_name = 'beforeprocess'
bucket = s3.Bucket(bucket_name)
file_name=[]
for obj in bucket.objects.all():
    file_name.append(obj.key)
for fn in file_name:
    read_image_from_s3(fn)
#

## 파일 경로지정을 위한 폴더 지정 ##
folder_path = "before/"
file_paths_list = glob.glob(folder_path + "*")
for path in file_paths_list:
  img = cv2.imread(path, cv2.IMREAD_GRAYSCALE)
  ret, dst = cv2.threshold(img, 127, 255, cv2.THRESH_BINARY_INV)
  result = reader.readtext(dst)

  #튜플 리스트로 만들어주기
  for i in range(len(result)):
    result[i] = list(result[i])
 
  #이름 따로 나오면 전처리
  if ' ' not in result[1][1]:
    result[1][1] = result[1][1] + ' ' + result[2][1]
    result[1][0][1] = result[2][0][1]
    result[1][0][2] = result[2][0][2] 
    del result[2]
  
  #나이 성별 붙어서 나오면 전처리
  if ' ' in result[2][1]:
    age, gender = result[2][1].split(' ')
    result.insert(2, [[[4, 64], [42, 64], [42, 94], [4, 94]], age, 0.9999998314126101])
    result.insert(3, [[[48, 66], [72, 66], [72, 90], [48, 90]], gender, 0.9272444468417831])
    del result[4]

  result = result[:8]

  extraction = []
  for i in result:
      extraction.append(i[1])

  print(extraction)
  
  extraction[4] = extraction[4].replace('_', '-')
  extraction[5] = extraction[5].replace('_', '-')
  if extraction[3]== 'm':
      extraction[3] = 'M'
  if extraction[4]== 'm':
      extraction[4] = 'M'
  extraction[5] = extraction[5].replace('.', ':')
  extraction[6] = extraction[6].replace('.', ':')

  extract = np.array(extraction)
  df = pd.DataFrame(extract, columns = ['extract'])
  extract = extract.tolist()
  print('after :', extract)

 #사진 이미지이름 지정 용 코드 -> 여기 수정
  patient_parts = ['Chest', 'Hand', 'Foot']
  for term in extract:
    if term == 'F':
      s = term
    if term == 'M':
      s = term
    #나이 a변수에 넣기
    if term.isdigit() and term >= '20' and term <= '50':
      a = int(term)
    if term in patient_parts:
      part = term

  prediction = predict(df)
  prediction = prediction.values.tolist()

  for pred in prediction: 
    idx = extract.index(pred[0])
    if result[idx][2]<0.4:
      continue
    left_upper = result[idx][0][0]
    right_lower = result[idx][0][2]
    try:
      if extract[idx][1] == ':':
        left_upper[0] = left_upper[0]-15
    except:
      pass
    right_lower[1] = right_lower[1]-5
    try:
      roi = img[left_upper[1]:right_lower[1], left_upper[0]:right_lower[0]]
    except:
      left_upper[1] = int(left_upper[1])
      left_upper[0] = int(left_upper[0])
      right_lower[1] = int(right_lower[1])
      right_lower[0] = int(right_lower[0])
      roi = img[left_upper[1]:right_lower[1], left_upper[0]:right_lower[0]]

    blurred_roi = cv2.blur(roi, (20, 20))
    img[left_upper[1]:right_lower[1], left_upper[0]:right_lower[0]] = blurred_roi

  ##이미지저장##
  cv2.imwrite("로컬경로/after/{}_{}_{}.jpeg".format(part, s, a), img)

  #afterprocess s3에 저장. 
  s3 = boto3.client('s3')
  file_name = "로컬경로/after/{}_{}_{}.jpeg".format(part, s, a)
  bucket = 'afterprocess'
  key = "{}_{}_{}.jpeg".format(part, s, a)

  s3.upload_file(file_name, bucket, key)

#스파크클러스터 종료  
spark.stop()


