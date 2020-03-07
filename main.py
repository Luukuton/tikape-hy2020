#! /usr/bin/python
# -*- encoding : utf-8 -*-
from datetime import datetime
import random
import sqlite3
import time

db = sqlite3.connect("tracking_system.db")
db.isolation_level = None
c = db.cursor()


def main():
    functions = {
        0: exit_app,
        1: create_database,
        2: add_location,
        3: add_client,
        4: add_packet,
        5: add_event,
        6: get_packet_events,
        7: get_client_events,
        8: get_events_during,
        9: benchmark,
        10: switch_indexes,
    }

    while True:
        print("\n 0: Poistu\n 1: Luo tietokanta ja taulukot\n 2: Lisää paikka\n 3: Lisää asiakas "
              "\n 4: Lisää paketti \n 5: Skannaa / Lisää tapahtuma"
              "\n 6: Hae tapahtumat seurantakoodin perusteella"
              "\n 7: Hae asiakkaan paketit ja niiden tapahtumien määrä"
              "\n 8: Hae tietyn paikan tapahtumien määrä tiettynä päivänä (vvvv.kk.pp)"
              "\n 9: Suorita tehokkuustesti\n10: Luo indeksit")
        n = input("\nValitse toiminto (0-10): ")
        if n.isdigit():
            n = int(n)
            if 0 <= n <= 10:
                run = functions.get(n)
                run()
            else:
                print("Toimintoa ei löydy. Valitse se väliltä 0-10: ")
        else:
            print("Toiminnon täytyy olla luku väliltä 0-10: ")


def exit_app():
    c.close()
    exit()


def create_database():
    try:
        c.execute("CREATE TABLE Locations ("
                  "id INTEGER PRIMARY KEY, "
                  "name VARCHAR UNIQUE NOT NULL)"
                  )
        c.execute("CREATE TABLE Clients ("
                  "id INTEGER PRIMARY KEY, "
                  "name VARCHAR UNIQUE NOT NULL)"
                  )
        c.execute("CREATE TABLE Packets ("
                  "id INTEGER PRIMARY KEY, "
                  "tracking_code VARCHAR UNIQUE NOT NULL, "
                  "client_id INTEGER NOT NULL)"
                  )
        c.execute("CREATE TABLE Events ("
                  "id INTEGER PRIMARY KEY, "
                  "packet_id INTEGER NOT NULL, "
                  "location_id INTEGER NOT NULL, "
                  "date INTEGER NOT NULL, "
                  "description TEXT DEFAULT 'Ei tapahtumakuvausta.')"
                  )
        print("Tietokanta luotu onnistuneesti!")

    except sqlite3.Error as e:
        handle_error(e)


def switch_indexes():
    try:
        c.execute("CREATE UNIQUE INDEX idx_cli_name ON Clients (name)")
        c.execute("CREATE UNIQUE INDEX idx_loc_name ON Locations (name)")
        c.execute("CREATE UNIQUE INDEX idx_t_code ON Packets (tracking_code)")
        c.execute("CREATE INDEX idx_date ON Events (packet_id)")
        print("Indeksit asetettu tietokantaan.")

    except sqlite3.Error as e:
        if str(e) == "index idx_cli_name already exists":
            c.execute("DROP INDEX idx_cli_name;")
            c.execute("DROP INDEX idx_loc_name;")
            c.execute("DROP INDEX idx_t_code;")
            c.execute("DROP INDEX idx_date;")
            print("Indeksit poistettu tietokannasta.")
        else:
            handle_error(e)


def add_location():
    try:
        location_name = input("\nAnna paikan nimi: ")
        if null_or_false(location_name):
            print("VIRHE: Paikan nimi ei voi olla tyhjä.")
            return

        c.execute("SELECT id FROM Locations WHERE name=?", [location_name])
        location_id = c.fetchone()

        if location_id is not None:
            print("VIRHE: Paikka on jo olemassa.")
        else:
            c.execute("INSERT INTO Locations (name) VALUES (?)", [location_name])
            print(f"Paikka {location_name} lisätty.")

    except sqlite3.Error as e:
        handle_error(e)


def add_client():
    try:
        client_name = input("\nAnna asiakkaan nimi: ")
        if null_or_false(client_name):
            print("VIRHE: Asiakkaan nimi ei voi olla tyhjä.")
            return

        c.execute("SELECT id FROM Clients WHERE name=?", [client_name])
        client_id = c.fetchone()

        if client_id is not None:
            print("VIRHE: Asiakas on jo olemassa.")
        else:
            c.execute("INSERT INTO Clients (name) VALUES (?)", [client_name])
            print(f"Asiakas {client_name} lisätty.")

    except sqlite3.Error as e:
        handle_error(e)


def add_packet():
    try:
        tracking_code = input("\nAnna paketin seurantakoodi: ")
        if null_or_false(tracking_code):
            print("VIRHE: Seurantakoodi ei voi olla tyhjä.")
            return

        c.execute("SELECT id FROM Packets WHERE tracking_code=?", [tracking_code])
        packet_id = c.fetchone()

        if packet_id is not None:
            print("VIRHE: Paketti annetulla seurantakoodilla on jo olemassa.")
        else:
            client_name = input("Anna asiakkaan nimi: ")
            if null_or_false(client_name):
                print("VIRHE: Asiakkaan nimi ei voi olla tyhjä.")
                return

            c.execute("SELECT id FROM Clients WHERE name=?", [client_name])
            client_id = c.fetchone()

            if client_id is None:
                print("VIRHE: Asiakasta ei ole olemassa.")
            else:
                c.execute("INSERT INTO Packets (tracking_code,client_id) VALUES (?,?)",
                          [tracking_code, client_id[0]])
                print(f"Paketti seurantakoodilla {tracking_code} lisätty.")

    except sqlite3.Error as e:
        handle_error(e)


def add_event():
    try:
        tracking_code = input("\nAnna paketin seurantakoodi: ")
        if null_or_false(tracking_code):
            print("VIRHE: Seurantakoodi ei voi olla tyhjä.")
            return

        c.execute("SELECT id FROM Packets WHERE tracking_code=?", [tracking_code])
        packet_id = c.fetchone()

        if packet_id is None:
            print("VIRHE: Annetun seurantakoodin pakettia ei ole olemassa.")
        else:
            location_name = input("Anna tapahtuman paikka: ")
            if null_or_false(location_name):
                print("VIRHE: Paikan nimi ei voi olla tyhjä.")
                return

            c.execute("SELECT id FROM Locations WHERE name=?", [location_name])
            location_id = c.fetchone()

            if location_id is None:
                print("VIRHE: Paikkaa ei ole olemassa.")
            else:
                description = input("Anna tapahtuman kuvaus: ")
                date = int(datetime.now().timestamp())
                c.execute("INSERT INTO Events (packet_id,location_id,date,description) "
                          "VALUES (?,?,?,?)", (packet_id[0], location_id[0], date, description))
                print(f"Tapahtuma lisätty.")

    except sqlite3.Error as e:
        handle_error(e)


def get_packet_events():
    try:
        tracking_code = input("\nAnna paketin seurantakoodi: ")
        c.execute("SELECT location_id, date, (SELECT name FROM Locations WHERE id=location_id), "
                  "description FROM Events "
                  "WHERE packet_id=(SELECT id FROM Packets WHERE tracking_code=?) "
                  "GROUP BY id ORDER BY date ASC", [tracking_code])
        events = c.fetchall()

        for e in events:
            print("{0}, {1}, {2}".format(datetime.fromtimestamp(e[1]).strftime("%Y.%m.%d %H:%M"), e[2], e[3]))

    except sqlite3.Error as e:
        handle_error(e)


def get_client_events():
    try:
        client_name = input("\nAnna asiakkaan nimi: ")
        c.execute("SELECT P.tracking_code, COUNT(P.id) FROM Events E, Packets P "
                  "WHERE (SELECT id FROM Clients WHERE name=?)=P.client_id "
                  "AND P.id=E.packet_id GROUP BY P.id", [client_name])
        events = c.fetchall()

        for obj in events:
            print("{0}, {1} tapahtumaa".format(obj[0], obj[1]))

    except sqlite3.Error as e:
        handle_error(e)


def get_events_during():
    try:
        name = input("\nAnna paikan nimi: ")
        c.execute("SELECT id FROM Locations WHERE name=?", [name])
        location_id = c.fetchone()

        if location_id is None:
            print("VIRHE: Paikkaa ei ole olemassa.")
        else:
            date_str = input("\nAnna päivämäärä: ")
            # Sekunnit päivässä: 86400. Käytetään siihen, että saadaan tarkka
            # päivän tasalukuinen Epoch Unix aika aikojen vertailun helpottamisen vuoksi.
            date_obj = int(datetime.strptime(date_str, '%Y.%m.%d').timestamp()) + (86400 / 2)
            date_obj = (date_obj - (date_obj % 86400))
            c.execute("SELECT COUNT(id) FROM Events WHERE (date - (date % 86400))=?", [date_obj])
            events = c.fetchone()
            print(f"Tapahtumien määrä: {events[0]}")

    except sqlite3.Error as e:
        handle_error(e)


def benchmark():
    try:
        # Poistaa aiemman tehokkuustestin data.
        c.execute("DELETE FROM Locations WHERE name LIKE '%BM_P%'")
        c.execute("DELETE FROM Clients WHERE name LIKE '%BM_A%'")
        c.execute("DELETE FROM Packets WHERE tracking_code LIKE '%BM_TR%'")
        c.execute("DELETE FROM Events WHERE description='BM_EVENT'")

        # Luo syötettävä data (etuliite BM tarkoittaa Benchmark).
        date = int(datetime.now().timestamp())
        locations, clients, packets, events = [], [], [], []

        for x in range(1, 1001):
            locations.append([f"BM_P{x}"])
            clients.append([f"BM_A{x}"])
            packets.append([f"BM_TR{x:09}", random.randrange(1, 1000)])
        for x in range(1, 1000001):
            events.append([random.randrange(1, 1000), random.randrange(1, 1000), date, "BM_EVENT"])

        # Aloita datan syöttäminen tietokantaan.
        c.execute("BEGIN TRANSACTION;")
        start = time.time()
        c.executemany("INSERT INTO Locations (name) VALUES (?)", locations)
        print(f"Sijainnit syötetty {round(time.time() - start, 6)} sekunnissa.")
        start = time.time()
        c.executemany("INSERT INTO Clients (name) VALUES (?)", clients)
        print(f"Asiakkaat syötetty {round(time.time() - start, 6)} sekunnissa.")
        start = time.time()
        c.executemany("INSERT INTO Packets (tracking_code,client_id) VALUES (?,?)", packets)
        print(f"Paketit syötetty {round(time.time() - start, 6)} sekunnissa.")
        start = time.time()
        c.executemany("INSERT INTO Events (packet_id,location_id,date,description) "
                      "VALUES (?,?,?,?)", events)
        print(f"Tapahtumat syötetty {round(time.time() - start, 6)} sekunnissa.")
        db.commit()

        # Aloita datan etsiminen tietokannasta.
        start = time.time()
        for x in range(1, 1000):
            client_id = random.randrange(1, 1000)
            c.execute("SELECT (SELECT name FROM Clients WHERE id=?), "
                      "COUNT(id) FROM Packets WHERE client_id=?", [client_id, client_id])
        print(f"1000 asiakkaan pakettien määrät haettu {round(time.time() - start, 6)} sekunnissa.")
        for x in range(1, 1000):
            packet_id = random.randrange(1, 1000)
            c.execute("SELECT (SELECT tracking_code FROM Packets WHERE packet_id=?), "
                      "COUNT(id) FROM Events WHERE packet_id=?", [packet_id, packet_id])
        print(f"1000 paketin tapahtumien määrät haettu {round(time.time() - start, 6)} sekunnissa.")

    except sqlite3.Error as e:
        handle_error(e)


def null_or_false(x):
    return x is None or not x


def handle_error(e):
    e = str(e)

    if e == "database is locked":
        print("VIRHE: Tietokanta on tällä hetkellä lukittu eli se on käytössä.")
    elif e == "table Locations already exists":
        print("Tietokanta ja sen taulut ovat jo olemassa!")
    else:
        print(e)


if __name__ == "__main__":
    main()
# End of file
