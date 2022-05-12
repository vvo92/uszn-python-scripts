from subprocess import Popen

while True:
    p = Popen("python " + 'app.py', shell=True)
    if p.wait() == 0:
        print('app.py полностью закончил работу без ошибок')
        break