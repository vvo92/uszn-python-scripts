import pathlib
import csv


def folders_create():
    RajsArr = ['1601', '1602', '1603', '1604', '1605', '1606', '1607', '1608', '1609', '1610', '1611', '1612', '1613', '1614', '1615', '1616',
               '1617', '1618', '1619', '1620', '1621', '1622', '1623', '1624', '1625', '1626', '1627', '1628', '1629', '1630', '1631', '1632', '1633']
    for Raj in RajsArr:
        pathlib.Path(
            f'./inbox/{Raj}').mkdir(parents=True, exist_ok=True)
    pathlib.Path(
        f'./outbox').mkdir(parents=True, exist_ok=True)


def mn_file_reader():
    mnArr = []
    for file_path in pathlib.Path('./inbox').glob('*/MN*.txt'):
        if file_path.is_file():
            print(f'Завантажуємо дані з: "{file_path.absolute()}"', end='\r')
            with open(file_path, 'r', encoding='866', newline='') as f1:
                for line in f1:
                    if line[4:7] == '000' and line[7] in ['4', '6', '7']:
                        rajN = line[0:4]
                        dopN = line[7:13]
                        pib = line[13:63].rstrip()
                        passp = line[63:77].rstrip()
                        ipn = line[77:87]
                        if [rajN, dopN, pib, passp, ipn] not in mnArr:
                            mnArr.append([rajN, dopN, pib, passp, ipn])
    return mnArr


def csv_export(raj, arrToExport):
    with open(f'./outbox/{raj}-result.csv', 'w', encoding='1251', newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=';',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in arrToExport:
            spamwriter.writerow(row)


def raj_file_reader():
    mnArr = mn_file_reader()
    for file_path in pathlib.Path('./inbox').glob('*/16*.csv'):
        if file_path.is_file():
            print(file_path.absolute())
            rajArr = []
            with open(file_path.absolute(), encoding='1251', newline='') as csvfile:
                csvreader = csv.reader(csvfile, delimiter=';', quotechar='"')
                for elem in csvreader:
                    subsN = elem[0]
                    relToOwner = elem[3]
                    pib = elem[4]
                    birthDay = elem[5]
                    ipn = elem[6]
                    if len(elem[8]) == 0:
                        passp = elem[7]

                    else:
                        passp = f'{elem[8]} {elem[7]}'
                    rajArr.append(
                        [subsN, relToOwner, pib, birthDay, ipn, passp])
            resultArr = [['Номер субсидії', 'Відношення до заявника', 'ПІБ', 'Дата народження',
                          'Інд. код (субсидії)', 'Паспорт (субсидії)', 'Номер району АСПОД', 'Номер справи в АСОПД', 'ПІБ', 'Паспорт (АСОПД)', 'Інд. код (АСОПД)']]
            for elem in rajArr:
                for elem2 in mnArr:
                    if elem[4] == elem2[4]:
                        resultArr.append([*elem, *elem2])

                    elif elem[5] == elem2[3] and [*elem, *elem2] not in resultArr:
                        resultArr.append([*elem, *elem2])

            csv_export(file_path.name[0:4], resultArr)


def main():
    folders_create()
    raj_file_reader()


if __name__ == '__main__':
    main()
