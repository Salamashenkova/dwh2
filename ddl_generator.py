import yaml  # ✅ ДОБАВЛЕНО!
import json

def generate_ddl(config_file, output_file):
    # ✅ Читаем ddl_config.yaml (Data Vault 2.0 формат!)
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    
    ddl_statements = []
    
    # ✅ ddl_config → HUB/SAT/LINK (НЕ tables!)
    for table_name, table_config in config['ddl_config'].items():
        table_type = table_config['type']
        schema = table_config['schema']
        business_key = table_config.get('business_key')
        hash_key = table_config.get('hash_key', f'hk_{table_name}')
        
        print(f"🔨 Generating {table_type}: {schema}.{table_name}")
        
        # 🎯 Data Vault 2.0 поля по типу
        if table_type == 'hub':
            fields = [
                f"{hash_key} BYTEA PRIMARY KEY",
                f"{business_key} VARCHAR NOT NULL",
                "load_dts TIMESTAMP DEFAULT NOW()",
                "rec_src VARCHAR DEFAULT 'source'"
            ]
            indexes = [f"CREATE INDEX idx_{table_name}_bk ON {schema}.{table_name}({business_key});"]
            
        elif table_type == 'sat':
            fields = [
                f"{hash_key[:-4]} BYTEA NOT NULL REFERENCES {schema}.{table_name[:-4]}({hash_key[:-4]})",
                "load_dts TIMESTAMP NOT NULL",
                "rec_src VARCHAR DEFAULT 'source'",
                "hashdiff BYTEA NOT NULL"
            ]
            # ✅ Атрибуты из config
            if 'attributes' in table_config:
                for attr in table_config['attributes'][:5]:  # Первые 5
                    fields.append(f"{attr} VARCHAR")
            fields.append("PRIMARY KEY ({hash_key[:-4]}, load_dts)")
            indexes = [f"CREATE INDEX idx_{table_name}_hashdiff ON {schema}.{table_name}(hashdiff);"]
            
        elif table_type == 'link':
            fields = [
                f"{hash_key} BYTEA PRIMARY KEY",
                f"{table_config['parent_keys'][0]['key']} BYTEA NOT NULL REFERENCES {schema}.{table_config['parent_keys'][0]['hub']}({table_config['parent_keys'][0]['key']})",
                f"{table_config['parent_keys'][1]['key']} BYTEA NOT NULL REFERENCES {schema}.{table_config['parent_keys'][1]['hub']}({table_config['parent_keys'][1]['key']})",
                "load_dts TIMESTAMP DEFAULT NOW()",
                "rec_src VARCHAR DEFAULT 'source'"
            ]
            indexes = [
                f"CREATE INDEX idx_{table_name}_hk1 ON {schema}.{table_name}({table_config['parent_keys'][0]['key']});",
                f"CREATE INDEX idx_{table_name}_hk2 ON {schema}.{table_name}({table_config['parent_keys'][1]['key']});"
            ]
        
        # ✅ CREATE TABLE
        ddl = f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} (\n  " + ",\n  ".join(fields) + "\n);"
        ddl_statements.append(ddl)
        
        # ✅ INDEXES
        ddl_statements.extend(indexes)
    
    # ✅ Запись в файл
    with open(output_file, 'w') as f:
        f.write("-- Data Vault 2.0 DDL (generated from ddl_config.yaml)\n")
        f.write("\n\n".join(ddl_statements))
    
    print(f"✅ DDL сгенерирован: {output_file}")

if __name__ == '__main__':
    generate_ddl('ddl_config.yaml', 'ddl_generated.sql')
