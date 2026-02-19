"""
Prompt Loader Utility

This module provides utilities to load task prompts from YAML configuration files.
Centralizing prompts in YAML files improves maintainability and allows non-developers
to modify prompts without touching code.
"""

import yaml
from pathlib import Path
from typing import Dict, Any, Optional


class PromptLoader:
    """
    Utility class for loading and formatting task prompts from YAML configuration files.
    
    Attributes:
        prompts_dir (Path): Path to the prompts directory containing YAML files
        _cache (Dict): Internal cache of loaded prompt files
    """
    
    def __init__(self, prompts_dir: Optional[str] = None):
        """
        Initialize the prompt loader.
        
        Args:
            prompts_dir: Path to prompts directory. Defaults to config/prompts relative to project root.
        """
        if prompts_dir is None:
            # Default to config/prompts from project root
            project_root = Path(__file__).parent.parent.parent
            self.prompts_dir = project_root / "config" / "prompts"
        else:
            self.prompts_dir = Path(prompts_dir)
        
        self._cache: Dict[str, Dict[str, Any]] = {}
    
    def load_prompt_file(self, filename: str) -> Dict[str, Any]:
        """
        Load a YAML prompt file and cache it.
        
        Args:
            filename: Name of the YAML file (e.g., "permit_prompts.yaml")
            
        Returns:
            Dictionary containing the parsed YAML content
            
        Raises:
            FileNotFoundError: If the prompt file doesn't exist
            yaml.YAMLError: If the file contains invalid YAML
        """
        # Return cached version if available
        if filename in self._cache:
            return self._cache[filename]
        
        file_path = self.prompts_dir / filename
        
        if not file_path.exists():
            raise FileNotFoundError(f"Prompt file not found: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                prompts = yaml.safe_load(f)
            
            # Cache the loaded prompts
            self._cache[filename] = prompts
            return prompts
            
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing YAML file {filename}: {e}")
    
    def get_prompt(self, filename: str, prompt_key: str, **format_kwargs) -> str:
        """
        Load a specific prompt template and format it with provided variables.
        
        Args:
            filename: Name of the YAML file containing the prompt
            prompt_key: Key path to the prompt (e.g., "permit_search.task_template")
            **format_kwargs: Variables to format into the prompt template
            
        Returns:
            Formatted prompt string ready to use
            
        Raises:
            KeyError: If the prompt key doesn't exist in the file
            
        Example:
            >>> loader = PromptLoader()
            >>> prompt = loader.get_prompt(
            ...     "permit_prompts.yaml",
            ...     "permit_search.task_template",
            ...     url="https://example.com",
            ...     start_date="01/01/2024",
            ...     end_date="01/31/2024"
            ... )
        """
        prompts = self.load_prompt_file(filename)
        
        # Navigate nested keys (e.g., "permit_search.task_template")
        keys = prompt_key.split('.')
        value = prompts
        
        for key in keys:
            if key not in value:
                raise KeyError(f"Prompt key '{prompt_key}' not found in {filename}")
            value = value[key]
        
        # Format the template with provided kwargs
        if isinstance(value, str) and format_kwargs:
            return value.format(**format_kwargs)
        
        return value
    
    def get_config(self, filename: str, config_key: str) -> Any:
        """
        Get configuration data from a prompt file.
        
        Args:
            filename: Name of the YAML file
            config_key: Key path to the configuration (e.g., "document_types.LIEN")
            
        Returns:
            Configuration value (can be dict, list, str, etc.)
            
        Example:
            >>> loader = PromptLoader()
            >>> lien_config = loader.get_config("lien_prompts.yaml", "document_types.LIEN")
            >>> print(lien_config['lookback_days'])  # Output: 30
        """
        prompts = self.load_prompt_file(filename)
        
        # Navigate nested keys
        keys = config_key.split('.')
        value = prompts
        
        for key in keys:
            if key not in value:
                raise KeyError(f"Config key '{config_key}' not found in {filename}")
            value = value[key]
        
        return value
    
    def clear_cache(self):
        """Clear the internal cache of loaded prompt files."""
        self._cache.clear()


# Global instance for convenience
_global_loader = PromptLoader()


def get_prompt(filename: str, prompt_key: str, **format_kwargs) -> str:
    """
    Convenience function to get a formatted prompt using the global loader.
    
    Args:
        filename: Name of the YAML file containing the prompt
        prompt_key: Key path to the prompt
        **format_kwargs: Variables to format into the prompt template
        
    Returns:
        Formatted prompt string
    """
    return _global_loader.get_prompt(filename, prompt_key, **format_kwargs)


def get_config(filename: str, config_key: str) -> Any:
    """
    Convenience function to get configuration using the global loader.
    
    Args:
        filename: Name of the YAML file
        config_key: Key path to the configuration
        
    Returns:
        Configuration value
    """
    return _global_loader.get_config(filename, config_key)
