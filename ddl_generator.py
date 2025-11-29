import yaml

def generate_ddl(config_file, output_file):
    # Читаем конфигурационный файл
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    
    ddl_statements = []
    
    # Проходим по таблицам в конфиге
    for table_name, table_config in config['tables'].items():
        database = table_config.get('database', 'default')  # По умолчанию база данных - default
        fields = table_config['fields']
        technical_fields = table_config.get('technical_fields', [])
        order_by = table_config.get('order_by', [])  # Поля для ORDER BY
        engine = table_config.get('engine', 'MergeTree()')  # Движок таблицы
        
        # Генерация CREATE TABLE
        ddl = f"CREATE TABLE {database}.{table_name} (\n"
        
        # Добавляем обычные поля
        field_definitions = []
        for field in fields:
            field_definition = f"  {field['name']} {field['type']}"
            field_definitions.append(field_definition)
        
        # Добавляем технические поля
        for tech_field in technical_fields:
            tech_definition = f"  {tech_field['name']} {tech_field['type']}"
            field_definitions.append(tech_definition)
        
        # Собираем все поля
        ddl += ",\n".join(field_definitions)
        ddl += "\n)"
        
        # Добавляем ENGINE и ORDER BY
        if order_by:
            ddl += f" ENGINE = {engine}\nORDER BY ({', '.join(order_by)});"
        else:
            ddl += f" ENGINE = {engine};"
        
        ddl_statements.append(ddl)
    
    # Записываем все DDL в файл
    with open(output_file, 'w') as f:
        f.write("\n".join(ddl_statements))
    
    print(f"DDL успешно сгенерирован в файл: {output_file}")

# Запускаем генератор
if __name__ == '__main__':
    generate_ddl('ddl_config.yaml', 'generated_ddl.sql')
