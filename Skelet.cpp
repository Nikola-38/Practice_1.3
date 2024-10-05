#include <iostream>
#include <map>
#include <sstream>
#include <string>

using namespace std;

struct Node {
    string key;
    string value;
    Node* next;
};

struct Table {
    Node* head;

        Table() {
        head = nullptr; // Инициализация указателя head
    }

    void insert(const string& key, const string& value) {
        Node* current = head;
        while (current) {
            if (current->key == key) {
                current->value = value; // Обновляем значение, если ключ уже существует
                return;
            }
            current = current->next;
        }
        addHead(key, value); // Вставляем новый элемент
    }

    void display() {
        Node* current = head;
        while (current) {
            cout << "{" << current->key << ": " << current->value << "} ";
            current = current->next;
        }
        cout << endl;
    }

    string search(const string& key) {
        Node* current = head;
        while (current) {
            if (current->key == key) {
                return current->value; // Возвращаем значение, если ключ найден
            }
            current = current->next;
        }
        return ""; // Ключ не найден
    }

    void remove(const string& key) {
        if (!head) return;
        if (head->key == key) {
            deleteHead();
            return;
        }
        Node* current = head;
        while (current->next) {
            if (current->next->key == key) {
                Node* temp = current->next;
                current->next = current->next->next; // Удаляем элемент
                delete temp;
                return;
            }
            current = current->next;
        }
    }

private:
    void addHead(const string& key, const string& value) {
        Node* newNode = new Node{key, value, head};
        head = newNode;
    }

    void deleteHead() {
        if (!head) return;
        Node* temp = head;
        head = head->next;
        delete temp;
    }
};

struct Database {
    map<string, Table> tables;

    void createTable(const string& tableName) {
        tables[tableName] = Table();
        cout << "Таблица '" << tableName << "' создана." << endl;
    }

    void insertInto(const string& tableName, const string& key, const string& value) {
        if (tables.find(tableName) != tables.end()) {
            tables[tableName].insert(key, value);
            cout << "Вставлено в '" << tableName << "': {" << key << ": " << value << "}" << endl;
        } else {
            cout << "Таблица '" << tableName << "' не найдена." << endl;
        }
    }

    void selectFrom(const string& tableName) {
        if (tables.find(tableName) != tables.end()) {
            cout << "Содержимое таблицы '" << tableName << "': ";
            tables[tableName].display();
        } else {
            cout << "Таблица '" << tableName << "' не найдена." << endl;
        }
    }

    void deleteFrom(const string& tableName, const string& key) {
        if (tables.find(tableName) != tables.end()) {
            tables[tableName].remove(key);
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


