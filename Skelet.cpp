#include <iostream>
#include <fstream>
#include <sstream>
#include <filesystem>
#include "json.hpp"
#include "rapidcsv.h"
#include "windows.h"

using namespace std;
using json = nlohmann::json;
namespace fs = filesystem;

// Определяем структуру для хранения названий колонок
struct Column {
    string colom; // Название колонки
    Column* next; // Указатель на следующую колонку
};

// Определяем структуру для хранения индексов выбранных колонок
struct IndexNode {
    size_t index; // Индекс колонки
    IndexNode* next; // Указатель на следующий индекс
};

struct TableJson {
    string schemeName;   // Имя схемы
    size_t tuplesLimit;  // Ограничение на количество кортежей
};

struct Table {
    string name;          // Имя таблицы
    string columnsFile;  // Файл с названиями колонок
    string dataFile;     // Файл для хранения данных (CSV)
    bool isLocked;       // Статус блокировки таблицы
    Table* next;        // Указатель на следующий элемент списка
};

// Функция для загрузки JSON из файла
void loadJson(const string& filename, json& j) {
    ifstream file(filename);
    if (!file.is_open()) {
        throw runtime_error("Error opening file: " + filename);
    }
    ostringstream ss;
    ss << file.rdbuf();
    j = json::parse(ss.str(), nullptr, false);
    if (j.is_discarded()) {
        throw runtime_error("JSON parsing error.");
    }
}

// Глобальный указатель на голову списка таблиц
Table* tableHead = nullptr;

// Проверка блокировки таблицы
bool isLocked(const string& tableName) {
    Table* current = tableHead;
    while (current) {
        if (current->name == tableName) {
            return current->isLocked;
        }
        current = current->next;
    }
    return false;
}

// Блокировка/разблокировка таблицы
void locker(const string& tableName) {
    Table* current = tableHead;
    while (current) {
        if (current->name == tableName) {
            current->isLocked = !current->isLocked; // Переключение состояния блокировки
            break;
        }
        current = current->next;
    }
}

// Проверка существования таблицы
bool isTableExist(const string& tableName) {
    Table* current = tableHead;
    while (current) {
        cout << "Проверяем таблицу: " << current->name << endl; // Для отладки
        if (current->name == tableName) {
            return true;
        }
        current = current->next;
    }
    return false;
}


// Копирование названий колонок
void copyColumnNames(const string& sourceFile, const string& targetFile) {
    ifstream src(sourceFile);
    ofstream tgt(targetFile);
    if (src && tgt) {
        string line;
        getline(src, line); // Читаем первую строку (названия колонок)
        tgt << line << endl; // Записываем в целевой файл
    }
}

// Вставка данных
void insert(const string& tableName, const string& values) {
    if (!isTableExist(tableName)) {
        cerr << "Таблица не существует!" << endl;
        return;
    }

    if (isLocked(tableName)) {
        cerr << "Таблица заблокирована!" << endl;
        return;
    }

    locker(tableName); // Блокируем таблицу

    // Получаем путь к файлу с данными (CSV)
    string dataFile = tableName + ".csv";
    rapidcsv::Document doc(dataFile, rapidcsv::LabelParams(0, -1)); // Читаем CSV файл

    // Получаем заголовки колонок
    vector<string> header = doc.GetColumnNames();
    size_t lineCount = doc.GetRowCount();

    // Обработка новых значений
    vector<string> newValues;
    stringstream ss(values);
    string value;
    while (getline(ss, value, ',')) {
        newValues.push_back(value);
    }

    // Проверяем на обновление
    bool updated = false;
    for (size_t i = 0; i < lineCount; ++i) {
        string key = doc.GetCell<string>(0, i); // Предположим, что первичный ключ в первой колонке
        if (stoi(key) == lineCount + 1) { // Здесь может быть ваша логика для нового ключа
            for (size_t j = 0; j < newValues.size(); ++j) {
                doc.SetCell<string>(j, i, newValues[j]); // Обновляем строки
            }
            updated = true;
            break;
        }
    }

    // Если запись не была обновлена, создаем новую
    if (!updated) {
        newValues.insert(newValues.begin(), to_string(lineCount + 1)); // Добавляем новый первичный ключ
        doc.InsertRow(lineCount, newValues); // Вставляем новую строку
    }

    // Сохраняем изменения обратно в файл
    doc.Save(dataFile);
    locker(tableName); // Разблокируем таблицу
}


// Функция для добавления таблицы в список
void addTable(const string& name, const string& columnsFile, const string& dataFile) {
    Table* newTable = new Table{name, columnsFile, dataFile, false, nullptr};
    newTable->next = tableHead;
    tableHead = newTable;
}

// Функция для создания файлов на основе структуры JSON
void createFiles(const json& structure) {
    for (const auto& [tableName, columns] : structure.items()) {
        // Создание файла блокировки
        ofstream lockFile(tableName + "_lock.txt");
        lockFile << "Status: unlocked\n";  // Запись состояния блокировки

        // Создание специальной колонки для первичного ключа
        ofstream csvFile(tableName + ".csv");
        csvFile << "PrimaryKey,";  // Заголовок для первичного ключа

        // Запись названий колонок
        for (size_t i = 0; i < columns.size(); ++i) {
            csvFile << columns[i].get<string>();
            if (i < columns.size() - 1) {
                csvFile << ",";
            }
        }
        csvFile << endl;  // Завершение строки CSV

        // Файл для хранения уникального первичного ключа
        ofstream keyFile(tableName + "_primary_key.txt");
        keyFile << "Primary Key: 1";  // Начальное значение первичного ключа

        // Добавление узла таблицы в список
        addTable(tableName, tableName + "_columns.txt", tableName + ".csv");
    }
}

// Функция для создания схемы на основе JSON
void createScheme(const json& j) {
    // Проверка наличия необходимых ключей
    if (j.contains("name") && j.contains("tuples_limit")) {
        TableJson tableJson = { j["name"].get<string>(), j["tuples_limit"].get<size_t>() };

        // Обработка структуры таблицы
        if (j.contains("structure")) {
            createFiles(j["structure"]);
        }
    } else {
        throw runtime_error("JSON key not found.");
    }
}


// Функция для подсчета количества CSV файлов в заданной директории
size_t countCsv(const string& tableName) {
    size_t count = 0;
    for (const auto& entry : filesystem::directory_iterator(tableName)) {
        if (entry.path().extension() == ".csv") {
            count++;
        }
    }
    return count;
}

// Функция для получения индекса колонки по её имени
size_t findColumnIndex(const string& header, const string& columnName) {
    istringstream headerStream(header);
    string column;
    size_t index = 0;
    while (getline(headerStream, column, ',')) {
        if (column == columnName) {
            return index;
        }
        index++;
    }
    return (size_t)-1; // Возвращаем -1, если не найдено
}

// Функция для получения значения колонки по имени
string getColumnValue(const string& line, const string& columnName, const string& headerLine) {
    size_t index = findColumnIndex(headerLine, columnName);
    if (index != (size_t)-1) {
        stringstream lineStream(line);
        string value;
        for (size_t i = 0; i <= index; ++i) {
            getline(lineStream, value, ',');
        }
        return value; // Возвращаем значение колонки
    }
    return ""; // Если колонка не найдена
}

// Функция для проверки условия WHERE
bool checkWhereCondition(const string& line1, const string& line2, const string& where, const string& headerLine1, const string& headerLine2) {
    // Убираем лишние пробелы
    string trimmedWhere = where;
    trimmedWhere.erase(remove_if(trimmedWhere.begin(), trimmedWhere.end(), ::isspace), trimmedWhere.end());

    // Поиск позиции знака равенства
    size_t equalPos = trimmedWhere.find('=');
    if (equalPos == string::npos) {
        return false; // Неподдерживаемый формат условия
    }

    // Получаем левые и правые части условия
    string leftSide = trimmedWhere.substr(0, equalPos);
    string rightSide = trimmedWhere.substr(equalPos + 1);

    // Убираем кавычки, если они есть
    if (rightSide.front() == '\'' && rightSide.back() == '\'') {
        rightSide = rightSide.substr(1, rightSide.length() - 2);
    }

    // Определяем, из какой строки мы берем значение для левой стороны
    string table1Column, table2Column;
    size_t dotPos = leftSide.find('.');
    if (dotPos != string::npos) {
        string tableName = leftSide.substr(0, dotPos);
        string columnName = leftSide.substr(dotPos + 1);
        if (tableName == "таблица1") {
            table1Column = getColumnValue(line1, columnName, headerLine1);
        } else if (tableName == "таблица2") {
            table2Column = getColumnValue(line2, columnName, headerLine2);
        }
    }

    // Сравниваем значения
    return (rightSide == table1Column || rightSide == table2Column);
}

// Функция для вывода всей таблицы с заголовками
void printFullTable(const string& tableName) {
    ifstream csvFile(tableName + ".csv");
    if (!csvFile.is_open()) {
        cerr << "Ошибка: не удалось открыть файл " << tableName << ".csv." << endl;
        return;
    }

    string headerLine;
    getline(csvFile, headerLine); // Считываем заголовок
    cout << headerLine << endl; // Выводим заголовок

    string line;
    while (getline(csvFile, line)) {
        cout << line << endl; // Выводим строки таблицы
    }
}

// Функция для вывода таблицы по заданным названиям колонок
void printColumns(const string& tableName, Column* columnNames) {
    ifstream csvFile(tableName + ".csv");
    if (!csvFile.is_open()) {
        cerr << "Ошибка: не удалось открыть файл " << tableName << ".csv." << endl;
        return;
    }

    string headerLine;
    getline(csvFile, headerLine); // Считываем заголовок

    // Определяем индексы выбранных колонок
    IndexNode* selectedIndexes = nullptr;
    IndexNode* lastIndex = nullptr;
    istringstream headerStream(headerLine);
    string column;
    size_t index = 0;

    // Находим индексы колонок для вывода
    while (getline(headerStream, column, ',')) {
        for (Column* col = columnNames; col != nullptr; col = col->next) {
            if (column == col->colom) {
                IndexNode* newIndex = new IndexNode{index, nullptr};
                if (selectedIndexes == nullptr) {
                    selectedIndexes = newIndex; // Если это первая колонка
                } else {
                    lastIndex->next = newIndex; // Присоединяем новую колонку
                }
                lastIndex = newIndex; // Обновляем указатель на последний индекс
                break; // Выход из внутреннего цикла
            }
        }
        index++;
    }

    // Выводим заголовки выбранных колонок
    for (IndexNode* idx = selectedIndexes; idx != nullptr; idx = idx->next) {
        istringstream headerStream(headerLine);
        string headerColumn;
        size_t currentIndex = 0;

        while (getline(headerStream, headerColumn, ',')) {
            if (currentIndex == idx->index) {
                cout << (idx == selectedIndexes ? "" : ",") << headerColumn;
                break;
            }
            currentIndex++;
        }
    }
    cout << endl;

    // Перематываем файл к началу для считывания данных
    csvFile.clear();
    csvFile.seekg(0);
    getline(csvFile, headerLine); // Пропускаем заголовок

    // Выводим данные из выбранных колонок
    string line;
    while (getline(csvFile, line)) {
        istringstream lineStream(line);
        string value;
        index = 0;
        bool firstValue = true;

        for (IndexNode* idx = selectedIndexes; idx != nullptr; idx = idx->next) {
            // Пропускаем ненужные колонки
            while (index < idx->index) {
                getline(lineStream, value, ',');
                index++;
            }

            getline(lineStream, value, ','); // Считываем значение нужной колонки

            // Убираем кавычки и пробелы
            if (!value.empty()) {
                if (value.front() == '\"' && value.back() == '\"') {
                    value = value.substr(1, value.size() - 2);
                }
                value.erase(remove(value.begin(), value.end(), ' '), value.end());
            }

            // Выводим значение колонки
            cout << (firstValue ? "" : ",") << value;
            firstValue = false; // Обозначаем, что это не первое значение
        }
        cout << endl; // Переход на новую строку после каждой записи
    }

    // Освобождаем память
    while (selectedIndexes != nullptr) {
        IndexNode* temp = selectedIndexes;
        selectedIndexes = selectedIndexes->next;
        delete temp;
    }
}


// Модифицируем функцию select
void select(const string& query) {
    string tableName;
    string columns;
    string whereToken;

    stringstream ss(query);
    string temp;
    ss >> temp; // Пропускаем "SELECT"

    // Считываем все столбцы до "FROM"
    getline(ss, columns, 'F'); // Читаем до "FROM"
    ss >> temp; // Пропускаем "FROM"
    ss >> tableName; // Читаем имя таблицы

    // Проверяем наличие WHERE
    size_t wherePos = string::npos;
    for (size_t i = 0; i < query.size() - 5; ++i) {
        if (query.substr(i, 5) == "WHERE") {
            wherePos = i;
            break;
        }
    }

    if (wherePos != string::npos) {
        whereToken = query.substr(wherePos + 6); // Получаем часть после WHERE
    }

    // Убираем лишние пробелы для имени таблицы
    while (!tableName.empty() && tableName[0] == ' ') {
        tableName = tableName.substr(1);
    }
    while (!tableName.empty() && tableName[tableName.size() - 1] == ' ') {
        tableName = tableName.substr(0, tableName.size() - 1);
    }

    // Убираем лишние пробелы для колонок
    while (!columns.empty() && columns[0] == ' ') {
        columns = columns.substr(1);
    }
    while (!columns.empty() && columns[columns.size() - 1] == ' ') {
        columns = columns.substr(0, columns.size() - 1);
    }

    // Проверяем наличие таблицы
    if (!fs::exists(tableName + ".csv")) {
        cerr << "Ошибка: таблица " << tableName << " не существует." << endl;
        return;
    }

    // Разбиваем названия колонок на связный список
    Column* columnNames = nullptr;
    Column* lastColumn = nullptr;

    string col;
    stringstream colStream(columns);
    while (colStream >> col) { // Читаем колонки, разделенные пробелами
        // Убираем пробелы из названия столбца
        while (!col.empty() && col[0] == ' ') {
            col = col.substr(1);
        }
        while (!col.empty() && col[col.size() - 1] == ' ') {
            col = col.substr(0, col.size() - 1);
        }

        if (!col.empty()) {
            Column* newColumn = new Column{col, nullptr};
            if (columnNames == nullptr) {
                columnNames = newColumn; // Если это первая колонка
            } else {
                lastColumn->next = newColumn; // Присоединяем новую колонку
            }
            lastColumn = newColumn; // Обновляем указатель на последнюю колонку
        }
    }

    // Проверяем, есть ли указанные столбцы
    if (columnNames != nullptr) {
        printColumns(tableName, columnNames);
    } else {
        printFullTable(tableName);
    }

    // Если указаны условия WHERE, добавьте сюда логику фильтрации
    if (!whereToken.empty()) {
        // Логика обработки условий WHERE
    }

    // Освобождаем память
    while (columnNames != nullptr) {
        Column* temp = columnNames;
        columnNames = columnNames->next;
        delete temp;
    }
}

// Основная функция
int main() {
    system("chcp 65001");
    string jsonFileName = "schema.json";  // Имя файла JSON с вашей схемой

    json j;
    try {
        loadJson(jsonFileName, j);
        createScheme(j);
        cout << "Схема '" << j["name"].get<string>() << "' успешно создана." << endl;
    } catch (const runtime_error& e) {
        cerr << "Ошибка: " << e.what() << endl;
        return 1;
    }

    // Основной цикл для ввода команд пользователем
    string command;
    while (true) {
        cout << "Введите команду (или 'exit' для выхода): ";
        getline(cin, command); // Удалил cin.ignore()

        cout << "Вы ввели команду: " << command << endl; // Для отладки

        if (command == "exit") {
            break;
        } else if (command.find("INSERT") == 0) {
            size_t tableNameEnd = command.find(' ', 7);
            string tableName = command.substr(7, tableNameEnd - 7);
            string values = command.substr(command.find(' ', tableNameEnd) + 1);

            // Удаление кавычек
            if (values.front() == '"' && values.back() == '"') {
                values = values.substr(1, values.size() - 2);
            }

            insert(tableName, values); // Вызов функции вставки
        } else if (command.find("SELECT") == 0) {
            select(command); // Вызов функции выбора
        } else {
            cout << "Неизвестная команда." << endl;
        }
    }
    return 0; // Перенесено сюда для выхода после завершения цикла
}
