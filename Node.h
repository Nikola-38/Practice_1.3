#pragma once
#include <string>
#include <fstream>

// Структура для колонок таблицы
struct ListNode {
    std::string column_name;
    ListNode* next;
};

// Структура для таблиц (связанный список)
struct Node {
    std::string table;
    Node* next;
    ListNode* column; // Указатель на список колонок таблицы
};

// Структура для описания схемы и таблиц
struct TableJson {
    std::string Name;      // Название схемы
    Node* Tablehead;       // Указатель на список таблиц
    int TableSize;         // Ограничение по количеству строк (tuples_limit)
};
