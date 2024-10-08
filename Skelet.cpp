#include <iostream>
#include <string>
#include <list>
#include <algorithm>
#include <sstream>

using namespace std;

// Структура для хранения записи
struct Record {
    list<string> values; // Поля записи в списке
};

// Структура для хранения таблицы
struct Table {
    list<string> columns; // Имена столбцов
    list<Record> records; // Список записей в таблице

    void insert(const list<string>& recordValues) {
        Record newRecord;
        newRecord.values = recordValues;
        records.push_back(newRecord);
    }

    void select(const list<string>& fieldNames) const {
        // Вывод заголовков
        for (const auto& field : fieldNames) {
            cout << field << "\t"; // Используем табуляцию для лучшего форматирования
        }
        cout << endl;

        // Для каждой записи из таблицы
        for (const auto& record : records) {
            auto it = record.values.begin();
            for (const auto& field : fieldNames) {
                auto colIt = find(columns.begin(), columns.end(), field);
                if (colIt != columns.end()) {
                    size_t index = distance(columns.begin(), colIt); // Получаем индекс столбца
                    advance(it, index); // Перемещаем итератор на нужную позицию
                    cout << *it << "\t"; // Печатаем значение поля из записи
                } else {
                    cout << "NULL\t"; // Если столбец не найден, выводим NULL
                }
            }
            cout << endl;
        }
    }

    void deleteRecord(const list<pair<string, string>>& criteria) {
        records.remove_if([&criteria, this](const Record& record) {
            for (const auto& [key, value] : criteria) {
                auto it = find(columns.begin(), columns.end(), key);
                if (it != columns.end()) {
                    size_t index = distance(columns.begin(), it);
                    auto recordIt = record.values.begin();
                    advance(recordIt, index);
                    if (*recordIt != value) {
                        return false;
                    }
                }
            }
            return true;
        });
    }
};

// Структура для хранения узлов хеш-таблицы
struct Node {
    string key;      // Ключ для таблицы
    Table* value;    // Указатель на таблицу
    Node* next;      // Указатель на следующий узел в цепочке
};

// Структура для хеш-таблицы
struct Hash {
    static const int tableSize = 10; // Размер таблицы
    Node* table[tableSize]; // Массив указателей на узлы

    // Конструктор
    Hash() {
        for (int i = 0; i < tableSize; i++) {
            table[i] = nullptr; // Инициализируем массив нулями
        }
    }

    // Хеш-функция
    int hashFunction(const string& key) {
        int hash = 0;
        for (char ch : key) {
            hash += ch; // Суммируем ASCII значения символов
        }
        return hash % tableSize; // Возвращаем индекс в пределах размера таблицы
    }

    // Добавление элемента
    void insert(const string& key, Table* value) {
        int index = hashFunction(key);
        Node* newNode = new Node{key, value, nullptr};

        if (!table[index]) {
            table[index] = newNode; // Если ячейка пуста, добавляем новый узел
        } else {
            Node* current = table[index];
            while (current) {
                if (current->key == key) {
                    current->value = value; // Обновляем значение, если ключ уже существует
                    delete newNode; // Удаляем временный узел
                    return;
                }
                if (!current->next) {
                    current->next = newNode; // Добавляем новый узел в конец цепочки
                    return;
                }
                current = current->next;
            }
        }
    }

    // Получение значения по ключу
    Table* get(const string& key) {
        int index = hashFunction(key);
        Node* current = table[index];
        while (current) {
            if (current->key == key) {
                return current->value; // Возвращаем значение, если ключ найден
            }
            current = current->next;
        }
        return nullptr; // Если ключ не найден
    }

    // Удаление элемента по ключу
    bool remove(const string& key) {
        int index = hashFunction(key);
        Node* current = table[index];
        Node* previous = nullptr;

        while (current) {
            if (current->key == key) {
                if (previous) {
                    previous->next = current->next; // Удаляем узел из цепочки
                } else {
                    table[index] = current->next; // Если это первый узел
                }
                delete current; // Освобождаем память
                return true;
            }
            previous = current;
            current = current->next;
        }
        return false; // Ключ не найден
    }
};

// Структура для базы данных
struct Database {
    Hash tables; // Хеш-таблица для хранения таблиц

    void createTable(const string& tableName, const list<string>& columnNames) {
        Table* newTable = new Table;
        newTable->columns = columnNames;
        tables.insert(tableName, newTable);
        cout << "Таблица '" << tableName << "' создана." << endl;
    }

    void insertInto(const string& tableName, const list<string>& recordValues) {
        Table* table = tables.get(tableName);
        if (table) {
            table->insert(recordValues);
            cout << "Данные вставлены в '" << tableName << "'." << endl;
        } else {
            cout << "Таблица '" << tableName << "' не найдена." << endl;
        }
    }

    void selectFrom(const string& tableName, const list<string>& fieldNames) {
        Table* table = tables.get(tableName);
        if (table) {
            cout << "Содержимое таблицы '" << tableName << "':" << endl;
            table->select(fieldNames);
        } else {
            cout << "Таблица '" << tableName << "' не найдена." << endl;
        }
    }

    void deleteFrom(const string& tableName, const list<pair<string, string>>& criteria) {
        Table* table = tables.get(tableName);
        if (table) {
            table->deleteRecord(criteria);
            cout << "Данные удалены из '" << tableName << "'." << endl;
        } else {
            cout << "Таблица '" << tableName << "' не найдена." << endl;
        }
    }
};

int main() {
    system("chcp 65001");
    Database db;
    string command;

    while (true) {
        cout << "Введите команду (или 'exit' для выхода): ";
        getline(cin, command);

        if (command == "exit") {
            break;
        }

        // Убираем лишние пробелы
        command.erase(remove(command.begin(), command.end(), '\r'), command.end());

        // Если команда CREATE
        if (command.substr(0, 6) == "CREATE") {
            size_t pos = command.find(" ");
            string tableName = command.substr(pos + 1, command.find(" ", pos + 1) - pos - 1);

            list<string> columnNames;
            pos = command.find("(");
            size_t endPos = command.find(")");

            string columns = command.substr(pos + 1, endPos - pos - 1);
            istringstream columnsStream(columns);
            string columnName;

            while (getline(columnsStream, columnName, ',')) {
                columnNames.push_back(columnName);
            }

            db.createTable(tableName, columnNames);
        } 
        // Если команда INSERT
        else if (command.substr(0, 6) == "INSERT") {
            size_t pos = command.find("INTO");
            string tableName = command.substr(pos + 5, command.find(" ", pos + 5) - pos - 5);

            pos = command.find("(");
            size_t endPos = command.find(")");

            string values = command.substr(pos + 1, endPos - pos - 1);
            list<string> recordValues;
            istringstream valuesStream(values);
            string value;

            while (getline(valuesStream, value, ',')) {
                recordValues.push_back(value);
            }

            db.insertInto(tableName, recordValues);
        }
        // Если команда SELECT
        else if (command.substr(0, 6) == "SELECT") {
            size_t pos = command.find("FROM");
            string tableName = command.substr(pos + 5, command.find(" ", pos + 5) - pos - 5);

            pos = command.find("(");
            size_t endPos = command.find(")");

            string fields = command.substr(pos + 1, endPos - pos - 1);
            list<string> fieldNames;
            istringstream fieldsStream(fields);
            string field;

            while (getline(fieldsStream, field, ',')) {
                fieldNames.push_back(field);
            }

            db.selectFrom(tableName, fieldNames);
        }
        // Если команда DELETE
        else if (command.substr(0, 6) == "DELETE") {
            size_t pos = command.find("FROM");
            string tableName = command.substr(pos + 5, command.find(" ", pos + 5) - pos - 5);

            list<pair<string, string>> criteria;
            size_t wherePos = command.find("WHERE");
            string conditions = command.substr(wherePos + 6);

            istringstream conditionsStream(conditions);
            string condition;
            while (getline(conditionsStream, condition, ',')) {
                size_t equalPos = condition.find("=");
                string key = condition.substr(0, equalPos);
                string value = condition.substr(equalPos + 1);
                criteria.push_back({key, value});
            }

            db.deleteFrom(tableName, criteria);
        }
        else {
            cout << "Неизвестная команда!" << endl;
        }
    }

    return 0;
}
