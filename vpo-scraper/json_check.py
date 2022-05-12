import json
import pathlib


def main():
    errJsonList = []
    for json_f in pathlib.Path('./jsons/dovydky').glob('*\*.json'):
        with open(json_f.absolute(), mode='r', encoding='utf-8-sig') as json_f:
            try:
                json_d = json.load(json_f)
            except:
                print(f'Некорректный json: {json_f.name}')
                errJsonList.append(json_f.name)
    if len(errJsonList) != 0:
        for jsonf in errJsonList:
            pathlib.Path(jsonf).unlink()


if __name__ == '__main__':
    main()
