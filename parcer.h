#pragma once
#include "Node.h" // структура таблиц
#include <iostream>
#include <string>
#include <fstream>
#include <filesystem> // директории
#include "json.hpp" // json

using namespace std;
using json = nlohmann::json; 
namespace fs = filesystem;


void DellDirectory(const fs::path& directoryPath); // удаление директории
void CreatesDirFiles(const fs::path& SchemePath, const json& structure, TableJson& json_table); // создание полной директории и файлов
void parser(TableJson& json_table); // парсинг схемы
