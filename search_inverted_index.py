#!/usr/bin/env python3

import sys
import os
from collections import defaultdict

class InvertedIndexSearcher:
    def __init__(self, index_file_path):
        """
        Inicializa el buscador cargando el índice invertido
        Args:
            index_file_path: Ruta al archivo del índice invertido generado por Spark
        """
        self.index = {}
        self.load_index(index_file_path)
    
    def load_index(self, index_file_path):
        """Carga el índice invertido desde el archivo CSV"""
        try:
            with open(index_file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    parts = line.split('\t')
                    if len(parts) >= 2:
                        object_type = parts[0]
                        documents = parts[1].split(',') if parts[1] else []
                        self.index[object_type] = set(documents)
            
            print(f"Índice cargado: {len(self.index)} tipos de objetos")
            print(f"Tipos disponibles: {', '.join(sorted(self.index.keys()))}")
            
        except FileNotFoundError:
            print(f"Error: No se encontró el archivo {index_file_path}")
            sys.exit(1)
        except Exception as e:
            print(f"Error cargando el índice: {e}")
            sys.exit(1)
    
    def search_single(self, object_type):
        """
        Busca documentos que contengan un tipo de objeto específico
        Args:
            object_type: Tipo de objeto a buscar (ej: 'person', 'vehicle')
        Returns:
            set: Conjunto de documentos que contienen el objeto
        """
        return self.index.get(object_type, set())
    
    def search_and(self, object_types):
        """
        Busca documentos que contengan TODOS los tipos de objetos especificados (AND)
        Args:
            object_types: Lista de tipos de objetos
        Returns:
            set: Documentos que contienen todos los objetos
        """
        if not object_types:
            return set()
        
        result = self.search_single(object_types[0])
        for obj_type in object_types[1:]:
            result = result.intersection(self.search_single(obj_type))
        
        return result
    
    def search_or(self, object_types):
        """
        Busca documentos que contengan CUALQUIERA de los tipos de objetos (OR)
        Args:
            object_types: Lista de tipos de objetos
        Returns:
            set: Documentos que contienen al menos uno de los objetos
        """
        result = set()
        for obj_type in object_types:
            result = result.union(self.search_single(obj_type))
        
        return result
    
    def search_not(self, include_types, exclude_types):
        """
        Busca documentos que contengan los objetos incluidos pero NO los excluidos
        Args:
            include_types: Tipos a incluir
            exclude_types: Tipos a excluir
        Returns:
            set: Documentos filtrados
        """
        include_docs = self.search_or(include_types) if include_types else set(self.get_all_documents())
        exclude_docs = self.search_or(exclude_types) if exclude_types else set()
        
        return include_docs - exclude_docs
    
    def get_all_documents(self):
        """Obtiene todos los documentos únicos en el índice"""
        all_docs = set()
        for docs in self.index.values():
            all_docs.update(docs)
        return all_docs
    
    def get_statistics(self):
        """Obtiene estadísticas del índice"""
        stats = {}
        all_docs = self.get_all_documents()
        
        stats['total_object_types'] = len(self.index)
        stats['total_documents'] = len(all_docs)
        stats['object_frequency'] = {obj_type: len(docs) for obj_type, docs in self.index.items()}
        
        return stats
    
    def interactive_search(self):
        """Modo interactivo de búsqueda"""
        print("\n=== BUSCADOR DE ÍNDICE INVERTIDO ===")
        print("Comandos disponibles:")
        print("  search <objeto>                    - Buscar un objeto")
        print("  and <obj1> <obj2> ...             - Documentos con TODOS los objetos")
        print("  or <obj1> <obj2> ...              - Documentos con CUALQUIER objeto")
        print("  not <incluir1,incluir2> <excluir1,excluir2> - Incluir pero excluir otros")
        print("  list                              - Listar tipos disponibles")
        print("  stats                             - Mostrar estadísticas")
        print("  help                              - Mostrar ayuda")
        print("  quit                              - Salir")
        print()
        
        while True:
            try:
                query = input(">>> ").strip()
                if not query:
                    continue
                
                parts = query.split()
                command = parts[0].lower()
                
                if command == 'quit' or command == 'exit':
                    break
                
                elif command == 'help':
                    print("Ejemplos de uso:")
                    print("  search person")
                    print("  and person vehicle")
                    print("  or person door dumpster")
                    print("  not person,vehicle door")
                
                elif command == 'list':
                    types = sorted(self.index.keys())
                    print(f"Tipos disponibles ({len(types)}): {', '.join(types)}")
                
                elif command == 'stats':
                    stats = self.get_statistics()
                    print(f"Estadísticas del índice:")
                    print(f"  - Tipos de objetos: {stats['total_object_types']}")
                    print(f"  - Documentos únicos: {stats['total_documents']}")
                    print(f"  - Frecuencia por tipo:")
                    for obj_type, count in sorted(stats['object_frequency'].items(), 
                                                 key=lambda x: x[1], reverse=True):
                        print(f"    {obj_type}: {count} documentos")
                
                elif command == 'search' and len(parts) == 2:
                    obj_type = parts[1]
                    results = self.search_single(obj_type)
                    print(f"Documentos con '{obj_type}' ({len(results)}): {sorted(results)}")
                
                elif command == 'and' and len(parts) > 1:
                    obj_types = parts[1:]
                    results = self.search_and(obj_types)
                    print(f"Documentos con TODOS {obj_types} ({len(results)}): {sorted(results)}")
                
                elif command == 'or' and len(parts) > 1:
                    obj_types = parts[1:]
                    results = self.search_or(obj_types)
                    print(f"Documentos con CUALQUIERA de {obj_types} ({len(results)}): {sorted(results)}")
                
                elif command == 'not' and len(parts) == 3:
                    include_types = parts[1].split(',') if parts[1] != '-' else []
                    exclude_types = parts[2].split(',') if parts[2] != '-' else []
                    results = self.search_not(include_types, exclude_types)
                    print(f"Documentos que incluyen {include_types} pero excluyen {exclude_types} ({len(results)}): {sorted(results)}")
                
                else:
                    print("Comando no reconocido. Escribe 'help' para ver los comandos disponibles.")
                
                print()
                
            except KeyboardInterrupt:
                print("\n¡Hasta luego!")
                break
            except Exception as e:
                print(f"Error: {e}")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Buscador para índice invertido')
    parser.add_argument('index_file', help='Archivo del índice invertido (CSV generado por Spark)')
    parser.add_argument('--search', '-s', help='Buscar un tipo de objeto específico')
    parser.add_argument('--and', dest='and_search', nargs='+', help='Buscar documentos con TODOS los objetos')
    parser.add_argument('--or', dest='or_search', nargs='+', help='Buscar documentos con CUALQUIER objeto')
    parser.add_argument('--interactive', '-i', action='store_true', help='Modo interactivo')
    parser.add_argument('--stats', action='store_true', help='Mostrar estadísticas del índice')
    
    args = parser.parse_args()
    
    # Inicializar el buscador
    searcher = InvertedIndexSearcher(args.index_file)
    
    if args.stats:
        stats = searcher.get_statistics()
        print("=== ESTADÍSTICAS DEL ÍNDICE ===")
        print(f"Tipos de objetos: {stats['total_object_types']}")
        print(f"Documentos únicos: {stats['total_documents']}")
        print("\nFrecuencia por tipo:")
        for obj_type, count in sorted(stats['object_frequency'].items(), 
                                     key=lambda x: x[1], reverse=True):
            print(f"  {obj_type}: {count} documentos")
        return
    
    if args.search:
        results = searcher.search_single(args.search)
        print(f"Documentos con '{args.search}': {sorted(results)}")
        return
    
    if args.and_search:
        results = searcher.search_and(args.and_search)
        print(f"Documentos con TODOS {args.and_search}: {sorted(results)}")
        return
    
    if args.or_search:
        results = searcher.search_or(args.or_search)
        print(f"Documentos con CUALQUIERA de {args.or_search}: {sorted(results)}")
        return
    
    if args.interactive:
        searcher.interactive_search()
        return
    
    # Si no se especifica ninguna opción, entrar en modo interactivo
    searcher.interactive_search()

if __name__ == "__main__":
    main()
