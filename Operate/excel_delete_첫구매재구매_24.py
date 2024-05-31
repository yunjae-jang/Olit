import os

def DeleteAllfiles(filePath):
    if os.path.exists(filePath):
        for file in os.listdir(filePath):
            os.remove(os.path.join(filePath, file))
        return 'Removed all files successfully'
    else:
        return 'Directory not found'

print(DeleteAllfiles('C:/Users/김민경/파이썬파일/첫구매재구매/얼라인랩재구매/'))
DeleteAllfiles('C:/Users/김민경/파이썬파일/첫구매재구매/얼라인랩첫구매/첫구매고객수/')
DeleteAllfiles('C:/Users/김민경/파이썬파일/첫구매재구매/얼라인랩첫구매/회원첫구매고객수/')
DeleteAllfiles('C:/Users/김민경/파이썬파일/첫구매재구매/얼라인랩첫구매/첫구매/')


print(DeleteAllfiles('C:/Users/김민경/파이썬파일/첫구매재구매/와이브닝재구매/'))
DeleteAllfiles('C:/Users/김민경/파이썬파일/첫구매재구매/와이브닝첫구매/첫구매고객수/')
DeleteAllfiles('C:/Users/김민경/파이썬파일/첫구매재구매/와이브닝첫구매/회원첫구매고객수/')
DeleteAllfiles('C:/Users/김민경/파이썬파일/첫구매재구매/와이브닝첫구매/첫구매/')

print(DeleteAllfiles('C:/Users/김민경/파이썬파일/첫구매재구매/셀올로지재구매/'))
DeleteAllfiles('C:/Users/김민경/파이썬파일/첫구매재구매/셀올로지첫구매/첫구매고객수/')
DeleteAllfiles('C:/Users/김민경/파이썬파일/첫구매재구매/셀올로지첫구매/회원첫구매고객수/')
DeleteAllfiles('C:/Users/김민경/파이썬파일/첫구매재구매/셀올로지첫구매/첫구매/')

print(DeleteAllfiles('C:/Users/김민경/파이썬파일/첫구매재구매/닥터아망재구매/'))
DeleteAllfiles('C:/Users/김민경/파이썬파일/첫구매재구매/닥터아망첫구매/첫구매고객수/')
DeleteAllfiles('C:/Users/김민경/파이썬파일/첫구매재구매/닥터아망첫구매/회원첫구매고객수/')
DeleteAllfiles('C:/Users/김민경/파이썬파일/첫구매재구매/닥터아망첫구매/첫구매/')

