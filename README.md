### Repozytorium

- Folder **ansible-gen-data** zawiera pliki *Ansible playbook*, które służą do generowania danych i ich wstępnego przetworzenia.
- Folder **ansible-load** zawiera pliki *Ansible playbook*, które służą do importu danych do baz danych.
- Folder **db** zawiera foldery zawierające funkcje udf, schematy tabel oraz pliki baz danych potrzebne na utworzenie klastra baz danych.
- Folder **experiments** (wcześniej nazwany results) zawiera dane eksperymentalne, dane uczące na potrzeby uczenia maszynowego oraz wygenerowane wykresy z danych eksperymentalnych.
- Folder **tools** zawiera narzędzie do generowania danych, narzędzie do kompresowania wygenerowanych danych w celu ich importu do bazy Cassandra oraz narzędzie do przetworzenia wygenerowanych danych do postaci JSON.
- Folder **util** zawiera pomocnicze skrypty Pythona, z funkcjami wykorzystywanymi przez inne skypty.
- Plik **env-cass.json** - plik konfiguracyjny środowiska dla bazy danych Cassandra.
- Plik **env-mongo.json** - plik konfiguracyjny środowiska dla bazy danych MongoDB.
- Plik **env-postgress.json** - plik konfiguracyjny środowiska dla bazy danych PostgreSQL.
- Skrypt **load_data.py** - skrypt importujący dane do bazy danyhc przy komkretnym pliku konfiguracyjnym środowiska.
- Skrypt **run_main.py** - główny skrypt odtwarzający środowisko i uruchamiający poniższe skrypty na klastrze Hadoop YARN, dla konkrentej bazy danych.
    - Skrypt **run_main_cass.py** - skrypt wykonujący proces ETL, dla bazy danych Cassandra.
    - Skrypt **run_main_mongo.py** - skrypt wykonujący proces ETL, dla bazy danych MongoDB.
    - Skrypt **run_main_postgress.py** - skrypt wykonujący proces ETL, dla bazy danych PostgreSQL.
 


 





