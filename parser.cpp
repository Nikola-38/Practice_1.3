#include "parcer.h"


void DellDirectory(const fs::path& directoryPath) { // удаление директории
    if (fs::exists(directoryPath)) { // проверка, существует ли она вообще
        fs::remove_all(directoryPath); // удаление
    }
}

void CreatesDirFiles(const fs::path& SchemePath, const json& structure, TableJson& json_table){
    Node* Tablehead = nullptr;
    Node* TableTail = nullptr;

    for(const auto& table : structure.items()){
        fs::path tablePath = SchemePath / table.key();
        if (!fs::create_directory(tablePath)) {
                cerr << "Не удалось создать директорию: " << tablePath << endl;
                return;
            }
            cout << "Создана директория: " << tablePath << endl;
        
        ListNode* newTable = new Node{table.key(), nullptr, nullptr}; // создаём таблицу
        fs::current_path(tablePath); // переходим в папку таблицы
        string lock = table.key() + "_lock.txt"; // создаём файл блокировки
        ofstream file(lock);
        if (!file.is_open()) {
            cerr << "Не удалось открыть файл.\n";
        }
        file << "unlocked"; // по умолчанию разблокировано
        file.close();
        if (TableHead == nullptr) { // добавляем таблицу в список
            TableHead = newTable; // если список пустой, таблица будет и первой, и последней одновременно
            TableTail = newTable;
        }
        else {
            TableTail->next = newTable; // иначе добавляем новую таблицу в конец списка
            TableTail = newTable; 
        }
        
        string keyColumn = table.key() + "_pk"; // название специальной колонки
        ListNode* column_pk = new ListNode{keyColumn, nullptr}; // создаём список, где специальная колонка - первая
        newTable->column = column_pk; // присоединяем список колонок к таблице

        fs::path csvFilePath = tablePath / "TableJS.csv"; // создаём csv файл
        ofstream csvFile(csvFilePath);
        if (!csvFile.is_open()) {
            cerr << "Не удалось создать файл: " << csvFilePath << endl;
            return;
        }
        
        csvFile << keyColumn << ",";
        const auto& columns = table.value(); // запись колонок в файл, объект columns = названия
        for (size_t i = 0; i < columns.size(); ++i) { 
            csvFile << columns[i].get<string>(); // записываем названия без кавычек
            Node* newColumn = new Node{columns[i], nullptr}; // создаём новую колонку
            if (newTable->column == nullptr) { // если в таблице ещё нет колонок
                newTable->column = newColumn;
            }
            else {
                ListNode* lastColumn = newTable->column;
                while (lastColumn->next != nullptr) { // ищем последнюю колонку
                    lastColumn = lastColumn->next;
                }
                lastColumn->next = newColumn; // добавляем новую колонку в конец
            }
            if (i < columns.size() - 1) { // для последнего значения не нужна запятая
                csvFile << ",";
            }
        }

        csvFile << endl;
        csvFile.close();
        cout << "Создан файл: " << csvFilePath << endl;

        string pk = keyColumn + "_sequence.txt"; // создаём файл для хранения уникального первичного ключа
        ofstream filePk(pk);
        if (!filePk.is_open()) {
            cerr << "Не удалось открыть файл.\n";
        }
        filePk << "0";
        filePk.close();
    }
    json_table.tablehead = tableHead;
}


void parser(TableJson& json_table){
    string fileName = "schema.json";
    ifstream file(fileName);
    if(!file.is_open()){
        cout << "Не удалось открыть файл";
    }

    string json_include; //содержимое
    string line;
    while(getline(file, line)){
        json_include += line;
    }
    file.close();

    json parser_Json;
    parser_Json = json::parse(parser_Json);

    json_table.schemeName = parser_Json["name"]; // извлекаем имя схемы
    fs::path schemePath = fs::current_path() / json_table.schemeName; // формируем путь к директории
    removeDirectory(schemePath); // удаляем, чтобы заново создать директорию
    if (!fs::create_directory(schemePath)) { // проверка
        cerr << "Не удалось создать директорию: " << schemePath << endl;
        return;
    }
    cout << "Создана директория: " << schemePath << endl;
    if (parser_Json.contains("structure")) { // наполнение директории
        createDirectoriesAndFiles(schemePath, parser_Json["structure"], json_table);
    }
    json_table.tableSize = parser_Json["tuples_limit"]; // вытаскиваем ограничения по строкам
}

