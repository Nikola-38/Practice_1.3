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

// Функция для удаления директории и её содержимого
void removeDirectory(const fs::path& dirPath) {
    if (fs::exists(dirPath)) {
        fs::remove_all(dirPath);
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

#include <iostream>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

using namespace std;

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
void printColumns(const string& tableName, const vector<string>& columnNames) {
    ifstream csvFile(tableName + ".csv");
    if (!csvFile.is_open()) {
        cerr << "Ошибка: не удалось открыть файл " << tableName << ".csv." << endl;
        return;
    }

    string headerLine;
    getline(csvFile, headerLine); // Считываем заголовок

    // Получаем индексы колонок
    vector<size_t> columnIndices;
    istringstream headerStream(headerLine);
    string column;
    size_t index = 0;

    while (getline(headerStream, column, ',')) {
        for (const string& colName : columnNames) {
            if (column == colName) {
                columnIndices.push_back(index);
                break;
            }
        }
        index++;
    }

    // Выводим заголовки выбранных колонок
    for (size_t i = 0; i < columnIndices.size(); ++i) {
        cout << (i > 0 ? "," : "") << columnNames[i];
    }
    cout << endl;

    // Выводим данные из выбранных колонок
    string line;
    while (getline(csvFile, line)) {
        istringstream lineStream(line);
        string value;
        for (size_t i = 0; i < columnIndices.size(); ++i) {
            for (size_t j = 0; j <= columnIndices[i]; ++j) {
                getline(lineStream, value, ',');
            }
            cout << (i > 0 ? "," : "") << value;
        }
        cout << endl;
    }
}

// Модифицируем функцию select
void select(const string& query) {
    string table1, table2;
    string whereToken;
    bool isTwoTables = false;

    stringstream ss(query);
    string temp;
    ss >> temp; // Пропускаем "SELECT"
    ss >> temp; // Пропускаем "FROM"
    getline(ss, temp); // Получаем оставшуюся часть команды

    size_t wherePos = temp.length();
    for (size_t i = 0; i < temp.length(); ++i) {
        if (temp[i] == 'W' && temp.substr(i, 5) == "WHERE") {
            wherePos = i;
            break;
        }
    }

    if (wherePos < temp.length()) {
        whereToken = temp.substr(wherePos + 6);
        temp = temp.substr(0, wherePos);
    }

    size_t commaPos = temp.length();
    for (size_t i = 0; i < temp.length(); ++i) {
        if (temp[i] == ',') {
            commaPos = i;
            isTwoTables = true;
            break;
        }
    }

    if (isTwoTables) {
        table1 = temp.substr(0, commaPos);
        table2 = temp.substr(commaPos + 1);
    } else {
        table1 = temp;
    }

    // Убираем лишние пробелы
    while (table1.length() > 0 && table1[0] == ' ') {
        table1 = table1.substr(1);
    }

    if (isTwoTables) {
        while (table2.length() > 0 && table2[0] == ' ') {
            table2 = table2.substr(1);
        }
    }

    // Проверяем наличие первой таблицы
    if (!fs::exists(table1 + ".csv")) {
        cerr << "Ошибка: таблица " << table1 << " не существует." << endl;
        return;
    }

    // Вывод всей таблицы, если нет указанных колонок
    if (!whereToken.empty()) {
        // Ваша логика выборки по условиям
    } else {
        printFullTable(table1); // Если нет условий, выводим всю таблицу
    }

    // Если есть вторая таблица
    if (isTwoTables) {
        if (!fs::exists(table2 + ".csv")) {
            cerr << "Ошибка: таблица " << table2 << " не существует." << endl;
            return;
        }
        printFullTable(table2); // Выводим всю вторую таблицу
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
