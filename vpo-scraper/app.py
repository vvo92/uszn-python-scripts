import pathlib
import datetime
import asyncio
import aiohttp
import json


def folders_create(RajsArr):
    for Raj in RajsArr:
        pathlib.Path(
            f'./jsons/ips/{Raj}').mkdir(parents=True, exist_ok=True)
        pathlib.Path(
            f'./jsons/dovydky/{Raj}').mkdir(parents=True, exist_ok=True)


async def get_auth_token(CryptoSrvIP, UserLogin, UserPassword):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.ws_connect('ws://localhost:11111') as ws:
                await ws.send_json({'id': 2, 'command': "sign", 'data': "MDUzNzYwNjU=", 'pin': "3882", 'storeContent': "true", 'includeCert': "true", 'useStamp': "false"})
                Response = await ws.receive_json()
                CryptoAutographSignDataJson = Response['data']['data']
        except aiohttp.client_exceptions.ClientConnectorError:
            print(f'CryptoAutograph на локальном компьютере не запущено')
            raise SystemExit
    Url = f'http://{CryptoSrvIP}:96/idp/rest/user/login'
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(Url, json={'api': "authApi", 'username': UserLogin, 'password': UserPassword, 'sign': CryptoAutographSignDataJson, 'sandbox': 0}) as response:
                PhpSessid = response.cookies['PHPSESSID'].value
                AuthToken = {'PHPSESSID': PhpSessid}
                return AuthToken

        except aiohttp.client_exceptions.ClientConnectorError:
            print(
                f'Криптосервер по адресу {CryptoSrvIP} не запущено или отсутсвует интернет подключение!')
            raise SystemExit


def ips_link_gen(CryptoSrvIP, Raj, FromDate, TillDate):
    Region = Raj[0:2]
    DaysArr = []
    for x in range(0, (TillDate-FromDate).days + 1):
        DaysArr.append(
            (FromDate + datetime.timedelta(days=x)).strftime('%d.%m.%Y'))
    UrlsArr = []
    for Day in DaysArr:
        UrlsArr.append(
            f'http://{CryptoSrvIP}:96/idp/application/search?mode=list&region={Region}&district={Raj}&state=50&from={Day}&till={Day}')
    print(f'\r\nКоличество дней к загрузке: {len(UrlsArr)}')
    return UrlsArr


def dov_link_gen(CryptoSrvIP, Raj):
    UrlsArr = []
    for JsonFile in pathlib.Path(f'./jsons/ips/{Raj}').iterdir():
        if JsonFile.is_file() and JsonFile.name[:3] == 'IPS':
            try:
                with open(JsonFile.absolute()) as JsonF:
                    JsonData = json.load(JsonF)
                    if len(JsonData) != 0:
                        for elem in JsonData:
                            DovydkaNumb = elem['id']
                            Url = f'http://{CryptoSrvIP}:96/idp/rest/getview/{DovydkaNumb}'
                            if pathlib.Path(f'./jsons/dovydky/{Raj}/DOVYDKA_{Raj}_{DovydkaNumb}.json').exists() is not True:
                                if Url not in UrlsArr:
                                    UrlsArr.append(Url)
            except:
                print('Некорректные jsonы, проверь правильность логина и пароля и срока действия ключа')
                raise SystemExit
    print(f'\r\nКоличество справок к загрузке: {len(UrlsArr)}')
    return UrlsArr


async def get_request(Url, session, JsonFileName, ReqType):
    async with session.get(Url) as response:
        if ReqType == 'IPS':
            with open(f"./jsons/ips/{JsonFileName[4:8]}/{JsonFileName}", "wb") as out:
                async for chunk in response.content.iter_chunked(1024):
                    out.write(chunk)
        elif ReqType == 'DOVYDKA':
            with open(f"./jsons/dovydky/{JsonFileName[8:12]}/{JsonFileName}", "wb") as out:
                async for chunk in response.content.iter_chunked(1024):
                    out.write(chunk)
        print(f'Загружено: {Url}', end='\r')


async def fetch_link(AuthToken, UrlsArr, ConnectionsLimit, Raj, ReqType):
    conn = aiohttp.TCPConnector(limit=ConnectionsLimit)
    timeout = aiohttp.ClientTimeout(total=36000)
    async with aiohttp.ClientSession(cookies=AuthToken, connector=conn, timeout=timeout) as session:
        tasks = set()
        for Url in UrlsArr:
            if ReqType == 'IPS':
                JsonFileName = f'IPS_{Raj}_{Url[-10:]}.json'
            elif ReqType == 'DOVYDKA':
                DovID = Url.split('/')[-1]
                JsonFileName = f'DOVYDKA_{Raj}_{DovID}.json'
            task = asyncio.create_task(get_request(
                Url, session, JsonFileName, ReqType))
            tasks.add(task)
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    # ----------------------------------------НАСТРОКИ--------------------------
    # Для работы скрипта нужен Python версии 3.10.X и модуль aiohttp устанавливется через консоль:
    # pip install aiohttp
    CryptoSrvIP = '127.0.0.1'  # Ip криптосервера
    UserLogin = 'root'  # Логин
    UserPassword = 'root'  # Пароль
    # Можно задавать несколько районов, при наличии доступа
    # RajsArr = ['1601', '1602', '1603', '1604', '1605', '1606', '1607', '1608', '1609', '1610', '1611', '1612', '1613', '1614', '1615', '1616',
    #            '1617', '1618', '1619', '1620', '1621', '1622', '1623', '1624', '1625', '1626', '1627', '1628', '1629', '1630', '1631', '1632', '1633']
    RajsArr = ['1619']  # район
    FromDate = datetime.date(2022, 2, 24)  # Начальная дата
    TillDate = datetime.date.today()  # Конечная дата
    # Лимит одновременных запросов, выше ставить нет смысла - сервер ИВЦ не тянет. На таком количестве выдавало 4 ответа в секунду.
    ConnectionsLimit = 30
    # ---------------------------------------Конец-------------------------------

    folders_create(RajsArr)

    AuthToken = asyncio.run(get_auth_token(
        CryptoSrvIP, UserLogin, UserPassword))
    for Raj in RajsArr:
        print(f'Подготовка к загрузке района: {Raj}')
        UrlsIpsArr = ips_link_gen(CryptoSrvIP, Raj, FromDate, TillDate)
        start_time = datetime.datetime.now()
        asyncio.run(fetch_link(AuthToken, UrlsIpsArr,
                    ConnectionsLimit, Raj, 'IPS'))
        print(
            f'\r\nВремя загрузки ИПС: {datetime.datetime.now() - start_time}')
        UrlsDovArr = dov_link_gen(CryptoSrvIP, Raj)
        start_time = datetime.datetime.now()
        asyncio.run(fetch_link(AuthToken, UrlsDovArr,
                    ConnectionsLimit, Raj, 'DOVYDKA'))
        print(
            f'\r\nВремя загрузки cправок: {datetime.datetime.now() - start_time}')
