#include "select.h"

// Функция для разделения строки на таблицу и колонку по точке
void separationDot(const string& word, string& table, string& column, const TableJson& json_table) {
    bool dot = false;
    table.clear();
    column.clear();

    for (size_t i = 0; i < word.size(); i++) {
        if (word[i] == '.') {
            if (dot) {
                cerr << "Некорректная команда: несколько точек в слове.\n";
                return;
            }
            dot = true; // Нахождение точки — начало колонки
            continue;
        }
        
        if (word[i] == ',') {
            // Игнорируем запятую в строке
            continue;
        }
        
        // Разделение на таблицу и колонку
        if (!dot) {
            table += word[i]; // До точки — это таблица
        } else {
            column += word[i]; // После точки — это колонка
        }
    }

    if (!dot) { 
        cerr << "Некорректная команда: точка не найдена.\n";
        return;
    }

    // Проверка существования таблицы
    if (TableExist(table, json_table.Tablehead) == false) {
        cerr << "Таблица " << table << " не найдена.\n";
        return;
    }

    // Проверка существования колонки в таблице
    if (ExistColonk(table, column, json_table.Tablehead) == false) {
        cerr << "Колонка " << column << " в таблице " << table << " не найдена.\n";
        return;
    }

}



// Убираем кавычки из строки
string ignoreQuotes(const string& indication) {
    string slovo;
    for (size_t i = 0; i < indication.size(); i++) {
        if (indication[i] != '\'') {
            slovo += indication[i];
        }
    }
    return slovo;
}

// Проверяем наличие точки в строке
bool findDot(const string& indication) {
    bool dot = false;
    for (size_t i = 0; i < indication.size(); i++) {
        if (indication[i] == '.') {
            dot = true;
        }
    }
    return dot;
}

// Функция для обработки одного условия
bool processConditionString(const TableJson& json_table, const string& table, const string& column, const string& s) {
   if (!s.empty()){
    int cntCsv = findCsvFileCount(json_table, table);
        for (size_t i = 1; i <= cntCsv; i++) { // просматриваем все созданные файлы csv
            string filePath = "/mnt/c/Users/Николай/practice 2/Practice 1.3/" + json_table.Name + "/" + table + "/" + to_string(i) + ".csv";
            rapidcsv::Document doc(filePath); // открываем файл
            int columnIndex = doc.GetColumnIdx(column); // считываем индекс искомой колонки
            size_t cntRow = doc.GetRowCount(); // считываем количество строк в файле
            for (size_t i = 0; i < cntRow; ++i) {
                string cellValue = doc.GetCell<string>(columnIndex, i);
                if (cellValue == s) {
                    cout << "Сравнение с значением"<<endl;
                    cout << "Таблица "<<table<<"(" << column << "): " <<cellValue << " = "<< s << endl;
                    return true;
                }
                else{
                    cerr << "Нет такого значения в таблице";
                }
            }
        }
   }
    return false; // Если ничего не нашли
}

bool processConditionTable(const TableJson& json_table, const string& table1, const string& table2, const string& column1, const string& column2){
        bool condition = true;
        int cntCsv1 = findCsvFileCount(json_table, table1);
        int cntCsv2 = findCsvFileCount(json_table, table2);

        for (size_t iCsv = 1; iCsv <= cntCsv1; iCsv++) {
            for (size_t icsv = 1; icsv <= cntCsv2; icsv++){
                string filePath1 = "/mnt/c/Users/Николай/practice 2/Practice 1.3/" + json_table.Name + "/" + table1 + "/" + to_string(iCsv) + ".csv";
                rapidcsv::Document doc1(filePath1); // открываем файл
                int columnIndex1 = doc1.GetColumnIdx(column1); // считываем индекс искомой колонки
                size_t cntRow1 = doc1.GetRowCount(); // считываем количество строк в файле

                string filePath2 = "/mnt/c/Users/Николай/practice 2/Practice 1.3/" + json_table.Name + "/" + table2 + "/" + to_string(icsv) + ".csv";
                rapidcsv::Document doc2(filePath2); // открываем файл
                size_t cntRow2 = doc2.GetRowCount(); // считываем количество строк в файле
                int columnIndex2 = doc2.GetColumnIdx(column2); // считываем индекс искомой колонки
                if(cntRow1 == cntRow2){
                    for (size_t i = 0; i < cntRow1; ++i) { // проходимся по строкам
                    string value1 = doc1.GetCell<string>(columnIndex1, i);
                    string value2 = doc2.GetCell<string>(columnIndex2, i);
                    // Сравниваем значения колонок
                    if (value1 != value2) {
                        condition = false;
                        cerr << " Значения в таблицах не равны";
                    }
                    if(value1 == value2){
                        condition = true;
                        cout << "Сравнение таблиц"<<endl;
                        cout << "Таблица1 (" << column1 << "): " << value1 << " | Таблица2 (" << column2 << "): " << value2 << endl;  
                    }
                }
            } else {
                cerr << "Количество строк в таблицах не совпадает" << endl;
                return false;
            }
        }
    }

    return condition;
}


// Функция для выполнения кросс-соединения
void crossJoinAndFilter(const TableJson& json_table, const string& table1, const string& table2, const string& column1, const string& column2) {
    int csvCNT1 = findCsvFileCount(json_table, table1); 
    int csvCNT2 = findCsvFileCount(json_table, table2); 

    // Перебор файлов из таблицы 1
    for (size_t iCsv1 = 1; iCsv1 <= csvCNT1; ++iCsv1) {
        string filePath1 = "/mnt/c/Users/Николай/practice 2/Practice 1.3/MySchemaJson/CLASSTEAMS/" + to_string(iCsv1) + ".csv";
        rapidcsv::Document doc1(filePath1); 

        int columnIndex1 = doc1.GetColumnIdx(column1);
        if (columnIndex1 == -1) {
            cerr << "Ошибка: Столбец " << column1 << " не найден в таблице " << filePath1 << endl;
            return;
        }

        size_t rows1 = doc1.GetRowCount();
        if (rows1 == 0) {
            cerr << "Файл " << filePath1 << " пуст!" << endl;
            return;
        }

        // Перебор файлов из таблицы 2
        for (size_t iCsv2 = 1; iCsv2 <= csvCNT2; ++iCsv2) {
            string filePath2 = "/mnt/c/Users/Николай/practice 2/Practice 1.3/MySchemaJson/STUDENT/" + to_string(iCsv2) + ".csv";
            rapidcsv::Document doc2(filePath2); 

            int columnIndex2 = doc2.GetColumnIdx(column2);
            if (columnIndex2 == -1) {
                cerr << "Ошибка: Столбец " << column2 << " не найден в таблице " << filePath2 << endl;
                return;
            }

            size_t rows2 = doc2.GetRowCount();
            if (rows2 == 0) {
                cerr << "Файл " << filePath2 << " пуст!" << endl;
                return;
            }

            for (size_t r1 = 0; r1 < rows1; ++r1) {
                string val1 = doc1.GetCell<string>(columnIndex1, r1);

                for (size_t r2 = 0; r2 < rows2; ++r2) {
                    string val2 = doc2.GetCell<string>(columnIndex2, r2);
                    cout << "Таблица1 (" << column1 << "): " << val1 << " | Таблица2 (" << column2 << "): " << val2 << endl;
                }
            }
        }
    }
}


void select(const string& query, const TableJson& json_table) {
    istringstream iss(query);
    string slovo;

    // Считываем "SELECT"
    iss >> slovo; // "SELECT"
    if (slovo != "SELECT") {
        cerr << "Некорректная команда: отсутствует SELECT.\n";
        return;
    }

    // Считываем первую таблицу и колонку
    iss >> slovo; // table1.column1
    string table1, column1;
    separationDot(slovo, table1, column1, json_table);

    // Считываем вторую таблицу и колонку
    iss >> slovo; // table2.column2
    string table2, column2;
    separationDot(slovo, table2, column2, json_table);

    // Проверяем наличие "FROM"
    iss >> slovo; // "FROM"
    if (slovo != "FROM") {
        cerr << "Некорректная команда: отсутствует FROM.\n";
        return;
    }

    // Считываем таблицы
    iss >> slovo; // таблица 1
    if (slovo != table1) {
        cerr << "Некорректная команда: первая таблица не совпадает.\n";
        return;
    }

    iss >> slovo; // таблица 2
    if (slovo != table2) {
        cerr << "Некорректная команда: вторая таблица не совпадает.\n";
        return;
    }

    // Проверка на наличие "WHERE"
    if (!(iss >> slovo) || slovo != "WHERE") {
        // Если "WHERE" отсутствует, выполняем crossJoin
        crossJoinAndFilter(json_table, table1, table2, column1, column2);
        cout << "Выполняем cross join без условий.\n";
        return;
    }

    // Первое условие
    string slovo1, slovo2;
    iss >> slovo1; // table1.column1
    string t1, c1;
    separationDot(slovo1, t1, c1, json_table);  // Разделяем на таблицу и колонку

    iss >> slovo; // "="

    iss >> slovo2; //table2.column2

    bool firstCondition = false;

    string t2, c2;
    separationDot(slovo2, t2, c2, json_table);  // Разделяем вторую колонку

    // Проводим проверку на равенство колонок
    firstCondition = processConditionTable(json_table, t1, t2, c1, c2);

    // Далее проверка на AND / OR
    string oper;
    if (iss >> oper && (oper == "AND" || oper == "OR")) {

        string conditionValue2;
        iss >> slovo; // STUDENT.CURS (или другая колонка)
        string t3, c3;
        separationDot(slovo, t3, c3, json_table);  // Разделяем на таблицу и колонку

        iss >> slovo; // "="
        iss >> conditionValue2; // Значение или колонка для второго условия

        bool secondCondition = false;
        // если значение в кавычках, то это строка
        string cleanedconditionValue2 = ignoreQuotes(conditionValue2);
        secondCondition = processConditionString(json_table, t3, c3, cleanedconditionValue2);

        // Логика для оператора AND
        if (oper == "AND" && (firstCondition && secondCondition)) {
            crossJoinAndFilter(json_table, table1, table2, column1, column2);
        }
        // Логика для оператора OR
        else if (oper == "OR" && (firstCondition || secondCondition)) {
            crossJoinAndFilter(json_table, table1, table2, column1, column2);
        }
        else{
            cerr << "Условия не выполняются" << endl;
        }
    }
}
