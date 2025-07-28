winlog_table_schema = """
        winlogbeat (
        
        timestamp DateTime,
        event Nested (
                code String ,
                action String ,
                category String ,
                type String ,
                kind String ,
                created String ,
                module String ,
                dataset String ,
                provider String ,
                outcome String ,
                severity String ,
                duration String ,        
        ),
        host Nested (
                name String ,
                ),
        user String DEFAULT '' ,
        data String DEFAULT '',
)
"""

