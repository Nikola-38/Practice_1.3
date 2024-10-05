#include <iostream>
#include <sstream>
#include <string>

using namespace std;

// Структура для хранения ключа и значения
struct Node {
    string key;
    string value;
    Node* next; // Указатель на следующий элемент в цепочке
};

// Структура для хеш-таблицы
struct Hash {
    static const int tableSize = 10; // Размер таблицы
    Node* table[tableSize]; // Массив указателей на Node

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
    void insert(const string& key, const string& value) {
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
    string get(const string& key) {
        int index = hashFunction(key);
        Node* current = table[index];
        while (current) {
            if (current->key == key) {
                return current->value; // Возвращаем значение, если ключ найден
            }
            current = current->next;
        }
        return "Ключ не найден"; // Если ключ не найден
    }

    // Удаление элемента по ключу
    void remove(const string& key) {
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
                return;
            }
            previous = current;
            current = current->next;
        }
        cout << "Ключ не найден для удаления" << endl; // Если ключ не найден
    }
};

// Структура для базы данных
struct Database {
    Hash tables; // Используем хеш-таблицу для хранения таблиц

    void createTable(const string& tableName) {
        tables.insert(tableName, ""); // Вставляем пустое значение для создания таблицы
        cout << "Таблица '" << tableName << "' создана." << endl;
    }

    void insertInto(const string& tableName, const string& key, const string& value) {
        string existingTable = tables.get(tableName);
        if (existingTable != "Ключ не найден") {
            tables.insert(key, value); // Вставляем в хеш-таблицу
            cout << "Вставлено в '" << tableName << "': {" << key << ": " << value << "}" << endl;
        } else {
            cout << "Таблица '" << tableName << "' не найдена." << endl;
        }
    }

    void selectFrom(const string& tableName) {
        string existingTable = tables.get(tableName);
        if (existingTable != "Ключ не найден") {
            cout << "Содержимое таблицы '" << tableName << "': ";
            // Здесь можно добавить логику для отображения содержимого таблицы
            cout << existingTable << endl; // Примерный вывод
        } else {
            cout << "Таблица '" << tableName << "' не найдена." << endl;
        }
    }

    void deleteFrom(const string& tableName, const string& key) {
        string existingTable = tables.get(tableName);
        if (existingTable != "Ключ не найден") {
            tables.remove(key);
            cout << "Удалено из '" << tableName << "': " << key << endl;
        } else {
            cout << "Таблица '" << tableName << "' не найдена." << endl;
        }
    }
};

int main() {
    Database db;
    string command;

    while (true) {
        cout << "Введите команду (или 'exit' для выхода): ";
        getline(cin, command);

        if (command == "exit") {
            break;
        }

        istringstream iss(command);
        string action;
        iss >> action;

        if (action == "CREATE") {
            string tableName;
            iss >> tableName;
            db.createTable(tableName);
        } else if (action == "INSERT") {
            string tableName, key, value;
            iss >> tableName >> key >> value;
            db.insertInto(tableName, key, value);
        } else if (action == "SELECT") {
            string tableName;
            iss >> tableName;
            db.selectFrom(tableName);
        } else if (action == "DELETE") {
            string tableName, key;
            iss >> tableName >> key;
            db.deleteFrom(tableName, key);
        } else {
            cout << "Неизвестная команда!" << endl;
        }
    }

    return 0;
}
