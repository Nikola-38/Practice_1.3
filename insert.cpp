#include "insert.h"

bool TableExist(const string& tableName, Node* tableHead) { // проверка: существует ли таблица
    Node* current = tableHead; // указатель на голову списка таблиц
    while (current) {
        if (current->table == tableName) {
            return true; // таблица с указанным именем существует
        }
        current = current->next;
    }
    return false; // такой таблицы нет
}

bool isloker(const string& tableName, const string& schemeName) {
    // Формируем путь к файлу блокировки таблицы
    fs::path baseDir = fs::path(":/mnt/c/Users/Николай/practice 2/Practice 1.3") / schemeName / tableName;
    fs::path lockFile = baseDir / (tableName + "_lock.txt");

    // Открываем файл
    ifstream file(lockFile);
    if (!file.is_open()) {
        сerr << "Ошибка: не удалось открыть файл " << lockFile << ".\n";
    }

    string current;
    file >> current;
    file.close();
    if (current == "locked") {
        return true; // заблокирована
    }
    return false; // разблокирована
}

void copyNameColonk(const string& from_file, const string& to_file) { // копирование названия колонок
    string columns; // строка для колонок
    ifstream fileF(from_file); // открываем файл для чтения колонок
    if (!fileF.is_open()) {
        cerr << "Не удалось открыть файл.\n";
        return;
    }
    fileF >> columns;
    fileF.close();
    ofstream fileT(to_file); // открываем файл для записи колонок
    if (!fileT.is_open()) {
        cerr << "Не удалось открыть файл.\n";
        return;
    }
    fileT << columns << endl;
    fileT.close();
}

void loker(const string& tableName, const string& schemeName) {
    // Получаем базовый путь с использованием std::filesystem
    fs::path baseDir = fs::path(":/mnt/c/Users/Николай/practice 2/Practice 1.3") / schemeName / tableName;
    fs::path lockFile = baseDir / (tableName + "_lock.txt");

    // Проверяем, существует ли файл блокировки
    if (!fs::exists(lockFile)) {
        cerr << "Ошибка: файл не существует: " << lockFile << "\n";
        return;
    }

    ifstream fileIn(lockFile);
    if (!fileIn.is_open()) {
        std::cerr << "Не удалось открыть файл: " << lockFile << "\n";
        return;
    }

    string current;
    // Чтение текущего значения блокировки
    fileIn >> current;
    fileIn.close();

    ofstream fileOut(lockFile); // перезаписываем файл
    if (!fileOut.is_open()) {
        cerr << "Не удалось открыть файл для записи: " << lockFile << "\n";
        return;
    }

    if (current == "locked") {
        fileOut << "unlocked"; // если таблица заблокирована, меняем на разблокирована
    } else {
        fileOut << "locked"; // если была разблокирована, становится заблокирована
    }
    fileOut.close();
}

void insert(const string& command, TableJson json_table){
    instrinstream iss(command);
    string slovo;
    iss >> slovo;
    iss >> slovo;

    if (word != "INTO") {
    cerr << "Некорректная команда.\n";
    return;
    }

    string tableName; // имя таблицы из команды
    iss >> word; // название таблицы
    if (TableExist(slovo, json_table.tablehead) == false) {
        cerr << "Такой таблицы нет.\n";
        return;
    }

    tableName = slovo;
    iss >> slovo; // "VALUES"
    if (slovo != "VALUES") {
        cerr << "Некорректная команда.\n";
        return;
    }

    string values; // строка для вытаскивания значений вставки
    while (iss >> slovo) {
        values += slovo;
    }
    if (values.front() != '(' || values.back() != ')') {
        cerr << "Некорректная команда.\n";
        return;
    }

    if (isloker(tableName,json_table.schemeName) == true) { // проверяем, доступна ли таблица для работы
    cerr << "Таблица заблокирована.\n";
    return;
    }

    locker(tableName, json_table.schemeName); // блокируем таблицу для вставки
    int currentPK; // текущий первичный ключ
    fs::path PKFile = fs::path(":/mnt/c/Users/Николай/practice 2/Practice 1.3") / json_table.schemeName / tableName / (tableName + "_pk_sequence.txt");
    ifstream fileIn(PKFile);
    if (!fileIn.is_open()) {
        cerr << "Не удалось открыть файл.\n";
        return;
    }

    fileIn >> currentPK; // считываем ключ в переменную
    fileIn.close();
    ofstream fileOut(PKFile);
    if (!fileOut.is_open()) {
        cerr << "Не удалось открыть файл.\n";
        return;
    }
    currentPK++;
    fileOut << currentPK; // увеличиваем на 1 и записываем новый ключ в файл
    fileOut.close();

    int csvNumber = 1; // номер файла csv
    while (true) {
        fs::path csvFile = fs::path(":/mnt/c/Users/Николай/practice 2/Practice 1.3") / json_table.schemeName /  tableName / (to_string(csvNumber) + ".csv");
        ifstream fileIn(csvFile); // открываем csv файл
        if (!fileIn.is_open()) {
            ofstream fileOut(csvFile); // если такого файла нет, создаём новый
            if (!fileOut.is_open()) {
                cerr << "Не удалось открыть файл.\n";
                return;
            }
            fileOut.close();
        }
        else {
            fileIn.close();
        }

        rapidcsv::Document doc(csvFile); // считываем csv в doc
        if (doc.GetRowCount() < json_table.tableSize) { // если количество строк не превышает лимит
            break; // завершаем цикл
        }
        csvNumber++; // иначе продолжаем искать пустые файлы
    }

    fs::path csvFirst = fs::path (":/mnt/c/Users/Николай/practice 2/Practice 1.3") / json_table.schemeName / (tableName + "/1.csv");
    fs::path csvSecond = fs::path (":/mnt/c/Users/Николай/practice 2/Practice 1.3") / json_table.schemeName / tableName / (to_string(csvNumber) + ".csv");
    rapidcsv::Document doc_(csvSecond); // считываем содержимое файла с помощью rapid
    if (doc_.GetRowCount() == 0 && doc_.GetColumnCount() == 0) { // если текущий файл пуст
        copyColumnsName(csvFirst, csvSecond); // записываем в него названия колонок
    }

     ofstream csv(csvSecond, ios::app);
    if (!csv.is_open()) {
        cerr << "Не удалось открыть файл.\n";
        return;
    }

    csv << currentPK << ","; // записываем текущий первичный ключ
    for (int i = 0; i < values.size(); i++) {
    if (values[i] == '\'') {  // если встречаем одиночную кавычку
        i++; 
        while (i < values.size() && values[i] != '\'') {
            csv << values[i]; 
            i++;
        }
        // Проверяем, что за кавычкой не сразу идет ')', чтобы правильно разделить значения
        if (i + 1 < values.size() && values[i + 1] != ')') {
            csv << ",";  // если это не последнее значение, ставим запятую
        } else {
            csv << endl;  
            }
    }
    }
    csv.close();
    locker(tableName, json_table.schemeName); // разблокировка
}
