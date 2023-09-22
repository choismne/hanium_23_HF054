import os

# #after내의 파일들 삭제
delete_folder_path = '/Users/choismn/Desktop/test_sparkmodel/after' 
for filename in os.listdir(delete_folder_path):
  delete_file_path = os.path.join(delete_folder_path, filename)
  os.remove(delete_file_path)

# #before내의 파일들 삭제
# delete_folder_path = '/Users/choismn/Desktop/test_sparkmodel/before' 
# for filename in os.listdir(delete_folder_path):
#   delete_file_path = os.path.join(delete_folder_path, filename)
#   os.remove(delete_file_path)