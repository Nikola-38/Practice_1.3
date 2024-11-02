#include <iostream>
#include <fstream>
#include <sstream>
#include <filesystem>
#include "json.hpp"
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
    ifstream dataIn(dataFile);
    
    if (!dataIn.is_open()) {
        cerr << "Ошибка при открытии файла." << endl;
        locker(tableName); // Разблокируем таблицу перед выходом
        return;
    }

    string header;
    getline(dataIn, header); // Читаем заголовок

    // Используем массив для хранения строк (если возможно, иначе используйте список)
    string lines[1000]; // Здесь 1000 — это максимальное количество строк
    int lineCount = 0;

    string line;
    bool updated = false;
    int newPrimaryKey = 0;

    // Обработка каждой строки
    while (getline(dataIn, line)) {
        if (line.empty()) continue;

        istringstream rowStream(line);
        string key;
        getline(rowStream, key, ','); // Извлекаем первичный ключ

        // Если обновляем запись
        if (stoi(key) == newPrimaryKey) {
            line = to_string(newPrimaryKey) + "," + values; // Обновляем строку
            updated = true;
        }

        lines[lineCount++] = line; // Сохраняем строку
    }

    // Если запись не была обновлена, создаем новую
    if (!updated) {
        newPrimaryKey = (lineCount > 0) ? (stoi(lines[lineCount - 1].substr(0, lines[lineCount - 1].find(','))) + 1) : 1;
        lines[lineCount++] = to_string(newPrimaryKey) + "," + values; // Добавляем новую строку
    }

    // Закрываем входной файл
    dataIn.close();

    // Открываем файл для записи и записываем все строки
    ofstream dataOut(dataFile, ios::trunc); // Открываем файл для перезаписи
    if (!dataOut.is_open()) {
        cerr << "Ошибка при открытии файла для записи." << endl;
        locker(tableName); // Разблокируем таблицу перед выходом
        return;
    }

    dataOut << header << endl; // Записываем заголовок
    for (int i = 0; i < lineCount; ++i) {
        dataOut << lines[i] << endl; // Записываем все строки обратно
    }

    // Закрываем выходной файл
    dataOut.close();
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

// Функция для проверки условия
bool evaluateCondition(const string& left, const string& op, const string& right) {
    if (op == "=") {
        return left == right;
    } else if (op == ">") {
        return stoi(left) > stoi(right);
    } else if (op == "<") {
        return stoi(left) < stoi(right);
    } else if (op == "startsWith") {
        return left.rfind(right, 0) == 0; // Проверяем, начинается ли строка
    }
    return false;
}

// Функция для проверки условия WHERE
bool checkWhereCondition(const string& row1, const string& row2, const string& condition) {
    istringstream conditionStream(condition);
    string token;

    while (conditionStream >> token) {
        if (token == "AND" || token == "OR") {
            continue; // Пропускаем оператор
        }

        string left, op, right;
        conditionStream >> left >> op >> right;

        size_t dotPos = left.find('.');
        string table = left.substr(0, dotPos);
        string column = left.substr(dotPos + 1);

        if (table == "table1") {
            size_t colIndex = findColumnIndex(row1, column); // Предполагаем, что findColumnIndex определена
            string value = row1.substr(colIndex, row1.find(',', colIndex) - colIndex);
            if (!evaluateCondition(value, op, right)) {
                return false; // Условие не выполнено
            }
        } else if (table == "table2") {
            size_t colIndex = findColumnIndex(row2, column);
            string value = row2.substr(colIndex, row2.find(',', colIndex) - colIndex);
            if (!evaluateCondition(value, op, right)) {
                return false; // Условие не выполнено
            }
        }
    }
    return true; // Все условия выполнены
}

// Функция для выполнения кросс-джойна
void crossJoin(const string& table1, const string& column1, const string& table2, const string& column2, const string& whereCondition) {
    size_t fileCount1 = countCsv(table1);
    size_t fileCount2 = countCsv(table2);

    if (fileCount1 == 0 || fileCount2 == 0) {
        cout << "Ошибка: одна из таблиц не содержит CSV файлов." << endl;
        return;
    }

    for (size_t i = 0; i < fileCount1; ++i) {
        ifstream csvFile1(table1 + "/" + table1 + "_" + to_string(i) + ".csv");
        if (!csvFile1.is_open()) {
            cout << "Ошибка: не удалось открыть файл " << table1 + "_" + to_string(i) + ".csv." << endl;
            continue;
        }

        string headerLine;
        getline(csvFile1, headerLine);
        string line1;
        while (getline(csvFile1, line1)) {
            for (size_t j = 0; j < fileCount2; ++j) {
                ifstream csvFile2(table2 + "/" + table2 + "_" + to_string(j) + ".csv");
                if (!csvFile2.is_open()) {
                    cout << "Ошибка: не удалось открыть файл " << table2 + "_" + to_string(j) + ".csv." << endl;
                    continue;
                }

                string headerLine2;
                getline(csvFile2, headerLine2);
                string line2;
                while (getline(csvFile2, line2)) {
                    // Проверяем условие WHERE
                    if (checkWhereCondition(line1, line2, whereCondition)) {
                        cout << line1 << ", " << line2 << endl; // Выводим значения
                    }
                }
            }
        }
    }
}

// Функция для вывода всех данных из таблицы
void printAllData(const string& dataFile) {
    ifstream csvFile(dataFile);
    if (!csvFile.is_open()) {
        cout << "Ошибка: не удалось открыть файл " << dataFile << endl;
        return;
    }

    string line;
    bool isEmpty = true; // Флаг для проверки пустого файла

    // Выводим весь файл
    while (getline(csvFile, line)) {
        cout << line << endl;  // Печатаем каждую строку
        isEmpty = false; // Если хотя бы одна строка выведена, файл не пустой
    }

    if (isEmpty) {
        cout << "Ошибка: файл пуст." << endl; // Сообщаем о пустом файле
    }

    csvFile.close();
}

// Функция для вывода выбранных колонок
void printSelectedColumns(const string& dataFile, const string& selectPart) {
    ifstream csvFile(dataFile);
    if (!csvFile.is_open()) {
        cout << "Ошибка: не удалось открыть файл " << dataFile << endl;
        return;
    }

    string headerLine;
    getline(csvFile, headerLine); // Читаем заголовок
    cout << headerLine << endl; // Выводим заголовок

    // Разделяем запрашиваемые колонки
    istringstream selectStream(selectPart);
    string column;
    vector<string> selectedColumns;

    while (getline(selectStream, column, ' ')) {
        // Удаляем пробелы
        column.erase(remove(column.begin(), column.end(), ' '), column.end());
        if (!column.empty()) {
            selectedColumns.push_back(column);
        }
    }

    string line;
    // Выводим только выбранные колонки
    while (getline(csvFile, line)) {
        istringstream lineStream(line);
        string value;
        for (const string& col : selectedColumns) {
            size_t colIndex = findColumnIndex(headerLine, col); // Находим индекс колонки

            if (colIndex != string::npos) {
                // Пропускаем значения до индекса colIndex
                size_t currentIndex = 0;
                istringstream lineStreamCopy(line);
                while (getline(lineStreamCopy, value, ',')) {
                    if (currentIndex == colIndex) {
                        cout << value << " "; // Выводим только выбранные колонки
                        break; // Выходим из внутреннего цикла
                    }
                    currentIndex++;
                }
            }
        }
        cout << endl; // Переход на новую строку
    }

    csvFile.close();
}

// Основная функция
void select(const string& command) {
    // Находим позицию FROM
    size_t fromPos = command.find("FROM");
    if (fromPos == string::npos) {
        cout << "Ошибка: не указано 'FROM'." << endl;
        return;
    }

    // Извлекаем часть SELECT
    string selectPart = command.substr(6, fromPos - 6);
    string fromPart = command.substr(fromPos + 5);

    // Извлекаем имя таблицы
    string tableName;
    istringstream fromStream(fromPart);
    fromStream >> tableName; // Получаем имя первой таблицы

    // Если selectPart пустой, выводим всю таблицу
    if (selectPart.empty()) {
        cout << "Вывод данных из таблицы " << tableName << ":" << endl;
        string dataFile = tableName + ".csv";
        printAllData(dataFile);  // Вывод всей таблицы
        return;
    }

    cout << "Вывод данных из таблицы " << tableName << ":" << endl;
    printSelectedColumns(tableName + ".csv", selectPart); // Вызываем для выбора колонок
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
