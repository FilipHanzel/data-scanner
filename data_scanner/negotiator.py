from typing import List, Dict


class Negotiator:
    @classmethod
    def negotiate(cls, schemas: List[Dict[str, str]]) -> Dict[str, str]:
        result = {}

        for schema in schemas:
            for key, value in schema.items():
                if key not in result:
                    result[key] = value
                else:
                    first_value = result[key]
                    result[key] = cls._get_resolved_type(first_value, value)

        return result

    @staticmethod
    def _get_resolved_type(a: str, b: str) -> str:
        if a == "unknown":
            return b
        if b == "unknown":
            return a

        if a == "string" or b == "string":
            return "string"

        if a == "integer":
            if b == "integer":
                return "integer"
            if b == "float":
                return "float"
            return "string"

        if a == "float":
            if b in ("integer", "float"):
                return "float"
            return "string"

        if a == "boolean":
            if b == "boolean":
                return "boolean"
            return "string"

        if a == "json":
            if b == "json":
                return "json"
            return "string"

        if a == "date":
            if b == "date":
                return "date"
            if b == "timestamp":
                return "timestamp"
            return "string"

        if a == "timestamp":
            if b in ("timestamp", "date"):
                return "timestamp"
            return "string"
