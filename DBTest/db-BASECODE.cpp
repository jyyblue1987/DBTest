/************************************************************
	Project#1:	CLP & DDL
 ************************************************************/

#include "db-BASECODE.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <ctime>
#include <string>

#if defined(_WIN32) || defined(_WIN64)
#define strcasecmp _stricmp
#endif

int main(int argc, char** argv)
{
	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;

	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"\n");
		return 1;
	}

	rc = initialize_tpd_list();

	if (rc)
	{
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
	}
	else
	{
		rc = get_token(argv[1], &tok_list);

		/* Test code */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class,
				      tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}
    
		if (!rc)
		{
			rc = do_semantic(tok_list, argv[1]);
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					  (tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}

    /* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
      tmp_tok_ptr = tok_ptr->next;
      free(tok_ptr);
      tok_ptr=tmp_tok_ptr;
		}
	}

	return rc;
}

/************************************************************* 
	This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc=0,i,j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;
	
	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
	  memset ((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank, (, ), or a comma, then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((strcasecmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
				  if (KEYWORD_OFFSET+j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET+j >= F_SUM)
            t_class = function_name;
          else
					  t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
					  add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do 
			{
				temp_string[i++] = *cur++;
			}
			while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank or a ), then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
		         || (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
				case '(' : t_value = S_LEFT_PAREN; break;
				case ')' : t_value = S_RIGHT_PAREN; break;
				case ',' : t_value = S_COMMA; break;
				case '*' : t_value = S_STAR; break;
				case '=' : t_value = S_EQUAL; break;
				case '<' : t_value = S_LESS; break;
				case '>' : t_value = S_GREATER; break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
    else if (*cur == '\'')
    {
      /* Find STRING_LITERRAL */
			
      cur++;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((*cur) && (*cur != '\''));

      temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
      else /* must be a ' */
      {
        add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
        cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
        }
      }
    }
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}
			
  return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

  if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

void add_command_log(char *command)
{
	FILE *fp = fopen("db.log", "a");
	if (fp == NULL)
		return;

	if (g_tpd_list->db_flags == ROLLFORWARD_PENDING)
		return;
	//if (g_tpd_list->db_flags == NORMAL)
	//	return;

	std::time_t t = std::time(0);   // get time now
	std::tm* now = std::localtime(&t);

	fseek(fp, 0, SEEK_END);
	fprintf(fp, "%04d%02d%02d%02d%02d%02d \"%s\"\n", 
			now->tm_year + 1900,
			now->tm_mon + 1,
			now->tm_mday,
			now->tm_hour,
			now->tm_min,
			now->tm_sec,			
		command);
	fclose(fp);
}


void add_ddl_command(char *command)
{
	FILE *fp = fopen("db.log", "a");
	if (fp == NULL)
		return;

	std::time_t t = std::time(0);   // get time now
	std::tm* now = std::localtime(&t);

	fseek(fp, 0, SEEK_END);
	fprintf(fp, "%s\n", command);
	fclose(fp);
}

int do_semantic(token_list *tok_list, char* command)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
  token_list *cur = tok_list;


	if ((cur->tok_value == K_CREATE) &&
			((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DROP) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_INSERT) &&
		(cur->next != NULL) && (cur->next->tok_value == K_INTO))
	{
		printf("INSERT INTO statement\n");
		cur_cmd = INSERT;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_SELECT) &&
		((cur->next != NULL) && (cur->next->tok_value == S_STAR) || (cur->next != NULL) && (cur->next->tok_value == IDENT)))
	{
		printf("SELECT statement\n");
		cur_cmd = SELECT;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_DELETE) &&
		(cur->next != NULL) && (cur->next->tok_value == K_FROM))
	{
		printf("DELETE FROM statement\n");
		cur_cmd = DELETE;
		cur = cur->next;

		add_command_log(command);
	}
	else if ((cur->tok_value == K_UPDATE) &&
		(cur->next != NULL) && (cur->next->tok_value == IDENT))
	{
		printf("UPDATE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_BACKUP) &&
		(cur->next != NULL) && (cur->next->tok_value == K_TO))
	{
		printf("BACK TO statement\n");
		cur_cmd = BACKUP;
		cur = cur->next;
		
	}
	else if ((cur->tok_value == K_RESTORE) &&
		(cur->next != NULL) && (cur->next->tok_value == K_FROM))
	{
		printf("RESTORE FROM statement\n");
		cur_cmd = RESTORE;
		cur = cur->next;

	}
	else if (cur->tok_value == K_ROLLFORWARD)
	{
		printf("ROLLFORWARD statement\n");
		cur_cmd = ROLLFORWARD;
		cur = cur->next;
	}
	else
	{
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd != INVALID_STATEMENT)
	{
		switch(cur_cmd)
		{
			case CREATE_TABLE:
				rc = sem_create_table(cur);
				if (rc == 0)
					add_command_log(command);
				break;
			case DROP_TABLE:
				rc = sem_drop_table(cur);
				if (rc == 0)
					add_command_log(command);
				break;
			case LIST_TABLE:
				rc = sem_list_tables();				
				break;
			case LIST_SCHEMA:
				rc = sem_list_schema(cur);
				break;
			case INSERT:
				rc = sem_insert(cur);
				if (rc == 0)
					add_command_log(command);
				break;
			case UPDATE:
				rc = sem_update(cur);
				if (rc == 0)
					add_command_log(command);
				break;

			case SELECT:
				rc = sem_select(cur);
				break;
			case DELETE:
				rc = sem_delete(cur);
				if (rc == 0)
					add_command_log(command);
				break;
			case BACKUP:
				rc = sem_backup(cur);
				if (rc == 0)
				{
					char log[100] = "";
					sprintf(log, "BACKUP %s", cur->next->tok_string);					
					add_ddl_command(log);
				}
				break;
			case RESTORE:
				rc = sem_restore(cur);
				break;
			case ROLLFORWARD:
				rc = sem_rollforward(cur);
				break;
			default:
					; /* no action */
		}
	}
	
	return rc;
}

int sem_create_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];

	int record_size = 0;


	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for(i = 0; i < cur_id; i++)
						{
              /* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string)==0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
                /* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;
		
								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) &&
										  (cur->tok_value != K_NOT) &&
										  (cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
								  else
									{
										col_entry[cur_id].col_len = sizeof(int);
										
										if ((cur->tok_value == K_NOT) &&
											  (cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}	
										else if ((cur->tok_value == K_NOT) &&
											    (cur->next->tok_value == K_NULL))
										{					
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}
	
										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												  (cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												if (cur->tok_value == S_RIGHT_PAREN)
												{
 													column_done = true;
												}
												cur = cur->next;
											}

											record_size += sizeof(int) + 1;											
										}
									}
								}   // end of T_INT processing
								else
								{
									// It must be char() or varchar() 
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;
		
										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											cur = cur->next;
											
											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;
						
												if ((cur->tok_value != S_COMMA) &&
														(cur->tok_value != K_NOT) &&
														(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														  (cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
																	 (cur->next->tok_value == K_NULL))
													{					
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}
		
													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) &&															  (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
													  {
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
															record_size += col_entry[cur_id].col_len + 1;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));
	
				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + sizeof(cd_entry) *	tab_entry.num_columns;
					tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry,
							     (void*)&tab_entry,
									 sizeof(tpd_entry));
		
						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
									 (void*)col_entry,
									 sizeof(cd_entry) * tab_entry.num_columns);
	
						rc = add_tpd_to_list(new_entry);

						free(new_entry);

						char table_name[MAX_IDENT_LEN + 4];
						strcpy(table_name, tab_entry.table_name);
						strcat(table_name, ".tab");
						FILE *fp = fopen(table_name, "wb");
						int record_size = 0;
						fwrite(&record_size, sizeof(int), 1, fp);
						fclose(fp);
					}
				}
			}
		}
	}
  return rc;
}

int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);
			}
		}
	}

  return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

  return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
  {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			  (cur->tok_class != identifier) &&
				(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;
					
					if ((cur->tok_class != keyword) &&
						  (cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{ 
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
            printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
              fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 
						}

						/* Next, write the cd_entry information */
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}
	
						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

  return rc;
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
//	struct _stat file_stat;
	

  /* Open for read */
  if((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
    else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));
			
			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
//		_fstat(_fileno(fhandle), &file_stat);		
		
		fseek(fhandle, 0L, SEEK_END);
		int size = ftell(fhandle);

		printf("dbfile.bin size = %d\n", size);

		g_tpd_list = (tpd_list*)calloc(1, size);

		fseek(fhandle, 0L, SEEK_SET);


		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, size, 1, fhandle);
			//fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != size)
			{
				rc = DBFILE_CORRUPTION;
			}

		}
	}
    
	return rc;
}
	
int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
  else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
			  else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								     1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								     old_size - cur->tpd_size -
										 (sizeof(tpd_list) - sizeof(tpd_entry)),
								     1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
					 	g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
									 1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						   has already subtracted the cur->tpd_size, therefore it will
						   point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							   next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
										 old_size - cur->tpd_size -
										 ((char*)cur - (char*)g_tpd_list),							     
								     1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}

				
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}
	
	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}


int sem_insert(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	cur = t_list;
	int cur_id = 0;
	tpd_entry* tab_entry;

	if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
	{
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
		printf("invalid statement-Ln891\n");
	}
	else
	{
		cur = cur->next;
		if (cur->tok_value != K_VALUES)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			printf("hewwo\n");
		}
		else
		{
			cur = cur->next;
			
			
			if (cur->tok_value != S_LEFT_PAREN)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				printf("invalid statement-Ln911\n");
			}
			else
			{
				cur = cur->next;

				FILE* fp;
				char *filename = (char*)malloc(sizeof(tab_entry->table_name) + 4);

				strcpy(filename, tab_entry->table_name);
				strcat(filename, ".tab");
				if ((fp = fopen(filename, "r+")) == NULL)
				{
					printf("ERROR: unable to read table from file\n");
					return FILE_OPEN_ERROR;
				}

				int record_count = 0;
				fread(&record_count, sizeof(int), 1, fp);

				fseek(fp, 0, SEEK_END);

				cd_entry* columns = get_columns(tab_entry);

				int record_size = get_record_size(columns, tab_entry->num_columns);
				char *buffer = (char*)malloc(record_size * sizeof(char));
			
				int elements_written = 0;

				while (!rc)
				{
					if (elements_written >= tab_entry->num_columns)
					{						
						break;
					}
					int column_len = columns[elements_written].col_len + 1;
					memset(buffer, 0, column_len);

					if (cur->tok_value == S_COMMA)
					{
						cur = cur->next;
						continue;
					}
					else if (cur->tok_value == STRING_LITERAL)
					{
						if (strlen(cur->tok_string) == 0 || strlen(cur->tok_string) > (size_t)(column_len - 1))
						{
							rc = INVALID_COLUMN_LENGTH;
							cur->tok_value = INVALID_STATEMENT;
							printf("ERROR: invalid length for column: %s\n", columns[elements_written].col_name);
							return rc;
						}
						else if (columns[elements_written].col_type == T_CHAR || columns[elements_written].col_type == T_VARCHAR)
						{
							strcpy(buffer, cur->tok_string);
							fwrite(buffer, column_len, 1, fp);

							cur = cur->next;
							elements_written++;
						}
						else
						{

							printf("ERROR: invalid type in value: %s\n", cur->tok_string);
							rc = INVALID_TYPE_NAME;
							return rc;
						}
					}
					else if (cur->tok_value == INT_LITERAL)
					{
						if (columns[elements_written].col_type != T_INT)
						{
							printf("ERROR: invalid type in value: %s\n", cur->tok_string);
							rc = INVALID_TYPE_NAME;
							return rc;
						}
						else
						{
							int value = atoi(cur->tok_string);							
							memcpy(buffer, &value, sizeof(int));

							fwrite(buffer, column_len, 1, fp);
							cur = cur->next;
							elements_written++;
						}
					}
					else if (cur->tok_value == K_NULL)
					{
						if (columns[elements_written].not_null)
						{
							rc = NULL_INSERT;
							cur->tok_value = INVALID_STATEMENT;
						}
						else
						{
							memset(buffer, 1, column_len - 1);
							fwrite(buffer, column_len, 1, fp);

							cur = cur->next;
							elements_written++;
						}
					}
					else if (cur->tok_value == S_RIGHT_PAREN)
					{
						rc = 0;
						break;// we're done

					}
					else
					{
						rc = INVALID_STATEMENT;
						cur->tok_value = INVALID;
					}

				}

				fseek(fp, 0, SEEK_SET);
				record_count++;
				fwrite(&record_count, sizeof(int), 1, fp);

				printf("copying buffer to record in memory.\n");
				
				free(filename);
				//free(columns);
				free(buffer);
				fflush(fp);
				fclose(fp);
			}
		}
	}
	return rc;
}

cd_entry* get_columns(tpd_entry* tab_entry)
{
	char *pos = (char *)tab_entry;
	pos += sizeof(tpd_entry);
	cd_entry *entry = (cd_entry *)pos;

	return entry;

	//FILE* fhandle;
	//if ((fhandle = fopen("dbfile.bin", "rb")) == NULL)
	//{
	//	printf("unable to open file2\n");
	//	return NULL;
	//}

	//cd_entry* columns = (cd_entry*)calloc(tab_entry->num_columns, sizeof(cd_entry));
	//fseek(fhandle, sizeof(tpd_list), SEEK_SET);



	//for (int i = 0; i < tab_entry->num_columns; i++)
	//{
	//	fread((void*)&columns[i], sizeof(cd_entry), 1, fhandle);
	//}
	//fclose(fhandle);


	//return columns;
}

int get_record_size(cd_entry *columns, int count)
{
	int size = 0;
	for (int i = 0; i < count; i++)
	{
		size += (columns[i].col_len + 1);
	}

	return size;
}

token_list* get_where_for_update(token_list *cur, int &rc) 
{
	t_list * where_cur = NULL;
	t_list* counter = cur;
	bool where = false;
	while ((counter->tok_value != EOC && counter->tok_value != K_WHERE) && !rc && cur != NULL)
	{
		counter = counter->next;
		if (counter->tok_value != S_EQUAL)
		{
			rc = INVALID_STATEMENT;
			printf("ERROR: expected '=', got %s", counter->next->tok_string);
			counter->next->tok_value = INVALID;
		}
		counter = counter->next;
		if (counter->tok_value != STRING_LITERAL && counter->tok_value != INT_LITERAL && counter->tok_value != K_NULL)
		{
			rc = INVALID_STATEMENT;
			printf("ERROR: expected string literal, int literal, or null keyword, got %s\n", counter->tok_string);
		}
		counter = counter->next;
	}


	if (counter->tok_value == K_WHERE)
		where_cur = counter->next;
	

	return where_cur;
}



int sem_update(token_list *tok)
{
	token_list *cur = tok;
	int rc = 0;
	tpd_entry *tab_entry = NULL;
	
	cd_entry* columns = NULL;
	int num_updated = 0;

	if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
	{
		printf("ERROR: table %s does not exist\n", cur->tok_string);
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
	}
	else
	{
		columns = get_columns(tab_entry);
		cur = cur->next;
		if (cur->tok_value != K_SET)
		{
			printf("ERROR: Invalid statement, expected 'SET', got %s\n", cur->tok_string);
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			if (cur->tok_value != IDENT)
			{
				printf("ERROR: Invalid statement, expected identifier\n");
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
			}
			else
			{
				
				token_list *where_cur = get_where_for_update(cur, rc);

				if (rc == INVALID_STATEMENT)
				{
					cur->tok_value = INVALID;
					return 0;
				}

				cd_entry* columns = get_columns(tab_entry);
				int record_size = get_record_size(columns, tab_entry->num_columns);

				FILE* fp;
				char *filename = (char*)malloc(sizeof(tab_entry->table_name) + 4);

				strcpy(filename, tab_entry->table_name);
				strcat(filename, ".tab");
				if ((fp = fopen(filename, "r+")) == NULL)
				{
					printf("ERROR: unable to read table from file\n");
					return FILE_OPEN_ERROR;
				}

				int record_count = 0;
				fread(&record_count, sizeof(int), 1, fp);

				char *buffer = (char*)malloc(record_size * record_count * sizeof(char));
				fread(buffer, record_size, record_count, fp);

				int update_count = 0;

				for (int i = 0; i < record_count; i++)
				{
					// check if current record can be updated
					char *row = buffer + record_size * i;
					if (is_where_satisfied(row, tab_entry, columns, where_cur) == false)					
						continue;
					
					t_list* col_cur = cur;
					t_list *val_cur = NULL;

					int col_index = 0;
					while (col_cur->tok_value != EOC)
					{
						if (col_cur->tok_value == K_WHERE)
							break;

						if (col_cur->tok_value == S_EQUAL
							|| col_cur->tok_value == INT_LITERAL
							|| col_cur->tok_value == STRING_LITERAL
							|| col_cur->tok_value == K_NULL)
						{
							col_cur = col_cur->next;
							continue;
						}

						val_cur = col_cur->next->next;

						// update record
						int pos = get_data_pos(tab_entry, columns, col_cur->tok_string);
						int len = get_data_len(tab_entry, columns, col_cur->tok_string);
						if (pos < 0)
						{
							printf("ERROR: Invalid statement, expected 'SET', got %s\n", cur->tok_string);
							rc = INVALID_STATEMENT;
							cur->tok_value = INVALID;
							break;
						}

						memset(row + pos, 0, len);

						if (val_cur->tok_value == INT_LITERAL)
						{
							int base = atoi(val_cur->tok_string);
							memcpy(row + pos, &base, sizeof(int));
						}
						else if (val_cur->tok_value == STRING_LITERAL)
						{
							char *val = row + pos;
							strcpy(row + pos, val_cur->tok_string);
						}
						else if (cur->tok_value == K_NULL)
						{
							memset(row + pos, 1, len - 1);
						}

						col_cur = col_cur->next;//skip to next statement
						
					}

					update_count++;
				}

				fseek(fp, sizeof(int), SEEK_SET);
				fwrite(buffer, record_size, record_count, fp);

				fclose(fp);
				free(buffer);

				printf("%d rows updated\n", update_count);
			}
		}
	}

	return rc;
}

bool is_where_satisfied(char *buffer, tpd_entry *tab_entry, cd_entry* columns, t_list *where) {
	if (where == NULL)
		return true;

	t_list *cur = where;
	bool flag[10];
	int and_or = -1;

	int where_count = 0;

	while (cur->tok_value != EOC && cur->tok_value != K_ORDER )
	{
		if (cur->tok_value != IDENT)
		{
			if (cur->tok_value == K_OR || cur->tok_value == K_AND)
				and_or = cur->tok_value;

			cur = cur->next;
			continue;
		}

		int comparator = cur->next->tok_value;
		t_list* val_cur = cur->next->next;

		int pos = get_data_pos(tab_entry, columns, cur->tok_string);
		if (pos < 0)
		{
			val_cur->tok_value = INVALID;
			return false;
		}

		int len = get_data_len(tab_entry, columns, cur->tok_string);

		flag[where_count] = false;

		if (val_cur->tok_value == INT_LITERAL)
		{
			int val = 0;
			memcpy(&val, buffer + pos, sizeof(int));

			int base = atoi(val_cur->tok_string);

			if (comparator == S_EQUAL && val == base)
				flag[where_count] = true;

			if (comparator == S_GREATER && val > base)
				flag[where_count] = true;

			if (comparator == S_LESS && val < base)
				flag[where_count] = true;
		}
		else if (val_cur->tok_value == STRING_LITERAL)
		{
			char *val = buffer + pos;

			int ret = strcmp(val, val_cur->tok_string);
			if (comparator == S_EQUAL && ret == 0)
				flag[where_count] = true;

			if (comparator == S_GREATER && ret > 0)
				flag[where_count] = true;

			if (comparator == S_LESS && ret < 0)
				flag[where_count] = true;
		}
		else if (val_cur->tok_value == K_NULL)
		{
			char *val = buffer + pos;

			if (comparator == K_IS && is_null(val, len) )
				flag[where_count] = true;
		}

		cur = cur->next;//skip to next statement

		where_count++;
		
	}

	if (and_or < 0)
		return flag[0];

	if (and_or == K_OR)
		return flag[0] || flag[1];

	return flag[0] && flag[1];
}

int get_data_pos(tpd_entry *tab_entry, cd_entry* columns, char *name)
{
	int pos = 0;
	bool find = false;
	for (int i = 0; i < tab_entry->num_columns; i++)
	{
		if (strcasecmp(columns[i].col_name, name) == 0)
		{
			find = true;
			break;
		}

		pos += (columns[i].col_len + 1);		
	}

	return find ? pos : -1;	
}

int get_data_len(tpd_entry *tab_entry, cd_entry* columns, char *name)
{
	for (int i = 0; i < tab_entry->num_columns; i++)
	{
		if (strcasecmp(columns[i].col_name, name) == 0)
			return columns[i].col_len + 1;
	}

	return -1;
}

int get_column_index(tpd_entry *tab_entry, cd_entry* columns, char *name)
{
	for (int i = 0; i < tab_entry->num_columns; i++)
	{
		if (strcasecmp(columns[i].col_name, name) == 0)
			return i;
	}

	return -1;
}


int sem_select(token_list *tok)
{
	int rc = 0;
	int p_record_size = 0;
	tpd_entry *tab_entry = NULL;
	token_list *cur = tok;
	bool projection;
	int num_pcols = 0;
	cd_entry* p_columns = NULL;
	token_list* column_names = NULL;
	
	if (cur->tok_value != S_STAR)
	{
		int count = 0;
		projection = true;
		column_names = cur;
	}
	else
	{
		projection = false;
	}

	token_list *temp = cur;
	while (temp->tok_value != K_FROM && temp->tok_value != EOC )
	{		
		temp = temp->next;
	}

	token_list *from_cur = NULL;
	if(temp->tok_value == EOC )
	{
		rc = INVALID_COLUMN_NAME;
		cur->tok_value = INVALID;
		printf("INVALID STATEMENT: %s\n", cur->tok_string);
		return rc;
	}

	token_list* table_cur = temp->next;
	
	if ((tab_entry = get_tpd_from_list(table_cur->tok_string)) == NULL)
	{
		rc = TABLE_NOT_EXIST;
		table_cur->tok_value = TABLE_NOT_EXIST;
		return rc;
	}
	else
	{
		//loads file header
		cd_entry* columns = get_columns(tab_entry);
		token_list *where_cur = get_where_for_select(table_cur, rc);
		token_list *order_by_cur = get_order_by(table_cur, rc);
		int ascending_flag = get_ascending_flag(order_by_cur, rc);
			
		int record_size = get_record_size(columns, tab_entry->num_columns);

		FILE* fp;
		char *filename = (char*)malloc(sizeof(tab_entry->table_name) + 4);

		strcpy(filename, tab_entry->table_name);
		strcat(filename, ".tab");
		if ((fp = fopen(filename, "r+")) == NULL)
		{
			printf("ERROR: unable to read table from file\n");
			return FILE_OPEN_ERROR;
		}

		int record_count = 0;
		fread(&record_count, sizeof(int), 1, fp);

		char *buffer = (char*)malloc(record_size * record_count * sizeof(char));
		fread(buffer, record_size, record_count, fp);

		fclose(fp);

		print_name_records(tab_entry, columns, cur, projection);

		orderRecords(buffer, tab_entry, columns, ascending_flag, order_by_cur->tok_string, record_size, record_count);

		for (int i = 0; i < record_count; i++)
		{
			// check if current record can be updated
			char *row = buffer + record_size * i;
			if (is_where_satisfied(row, tab_entry, columns, where_cur) == false)
				continue;

			if (projection == true)
			{
				token_list* col_names = cur;

				while (col_names->tok_value != K_FROM) {
					if (col_names->tok_value == IDENT)
					{
						int pos = get_data_pos(tab_entry, columns, col_names->tok_string);
						if (pos < 0)
						{
							rc = INVALID_COLUMN_NAME;
							col_names->tok_value = INVALID;
							return rc;
						}

						int len = get_data_len(tab_entry, columns, col_names->tok_string);

						int col_index = get_column_index(tab_entry, columns, col_names->tok_string);
						int col_type = columns[col_index].col_type;

						if (is_null(row + pos, len))
						{
							printf("|% 16s", "NULL");
						}
						else if (col_type == T_INT)
						{
							int val = 0;
							memcpy(&val, row + pos, sizeof(int));
							printf("|% 16d", val);
						}
						else if (col_type == T_CHAR || col_type == T_VARCHAR)
						{
							printf("|% 16s", row + pos);
						}
						else if (col_type == K_NULL)
						{
							printf("|% 16s", "NULL");
						}
					}
					col_names = col_names->next;
				}
			}
			else {
				int pos = 0;
				for (int i = 0; i < tab_entry->num_columns; i++)
				{
					int col_type = columns[i].col_type;
					int len = columns[i].col_len + 1;

					if (is_null(row + pos, len))
					{
						printf("|% 16s", "NULL");
					}
					else if (col_type == T_INT)
					{
						int val = 0;
						memcpy(&val, row + pos, sizeof(int));
						printf("|% 16d", val);
					}
					else if (col_type == T_CHAR || col_type == T_VARCHAR)
					{
						printf("|% 16s", row + pos);
					}
					else if (col_type == K_NULL)
					{
						printf("|% 16s", "NULL");
					}

					pos += (columns[i].col_len + 1);
				}
			}

			printf("|\n");
		}
		free(buffer);
	}

	return rc;

}

int orderRecords(char *buff, tpd_entry *tab_entry, cd_entry *columns, int flag, char *sort_name, int record_size, int record_count)
{
	if (flag == 0)
		return 0;
	int pos = get_data_pos(tab_entry, columns, sort_name);
	if (pos < 0)
		return INVALID_COLUMN_NAME;

	int col_index = get_column_index(tab_entry, columns, sort_name);
	int col_type = columns[col_index].col_type;
	int len = get_data_len(tab_entry, columns, sort_name);

	if (col_type == K_NULL)
		return 0;

	char* temp = (char*)malloc(record_size);
	
	if (col_type == T_INT)
	{
		for (int i = 0; i < record_count - 1; i++)
		{
			char *row1 = buff + i * record_size;
			
			int val1 = 0;
			memcpy(&val1, row1 + pos, sizeof(int));
			
			for (int j = i + 1; j < record_count; j++)
			{
				char *row2 = buff + j * record_size;

				int val2 = 0;
				memcpy(&val2, row2 + pos, sizeof(int));

				if (flag == 1 && val1 > val2 
					|| flag == 2 && val1 < val2 )	// swap
				{
					memcpy(temp, row1, record_size);
					memcpy(row1, row2, record_size);
					memcpy(row2, temp, record_size);
				}
			}
		}
	}

	if (col_type == T_CHAR || col_type == T_VARCHAR)
	{
		for (int i = 0; i < record_count - 1; i++)
		{
			char *row1 = buff + i * record_size;

			char *buff1 = row1 + pos;
			
			for (int j = i + 1; j < record_count; j++)
			{
				char *row2 = buff + j * record_size;

				char *buff2 = row2 + pos;

				if (flag == 1 && strcmp(buff1, buff2) < 0 
					|| flag == 2 && strcmp(buff1, buff2) > 0 )	// swap
				{
					memcpy(temp, row1, record_size);
					memcpy(row1, row2, record_size);
					memcpy(row2, temp, record_size);
				}
			}
		}
	}

	free(temp);

	return 0;	
}

bool is_null(char *buf, int len)
{
	char *temp = (char *)malloc(len * sizeof(char));
	memset(temp, 1, len);
	int ret = memcmp(buf, temp, len - 1);
	free(temp);

	return ret == 0;
}

void print_split_line(int num_cols)
{
	for (int i = 0; i < num_cols - 1; i++)
	{
		printf("+----------------");
	}

	printf("+----------------+\n");
}

void print_name_records(tpd_entry *tab_entry, cd_entry* columns, token_list *cur, bool projection)
{
	//print header
	int num_cols = 0;
	if (projection == false)
	{
		num_cols = tab_entry->num_columns;

		print_split_line(num_cols);		

		for (int i = 0; i < num_cols; i++)
		{
			printf("|% 16s", columns[i].col_name);
		}
		printf("|\n");

	}
	else
	{
		token_list* col_names = cur;

		while (col_names->tok_value != K_FROM) {		
			if( col_names->tok_value == IDENT )
				num_cols++;
			col_names = col_names->next;			
		}

		print_split_line(num_cols);

		col_names = cur;

		while (col_names->tok_value != K_FROM) {
			if (col_names->tok_value == IDENT)
				printf("|% 16s", col_names->tok_string);
			col_names = col_names->next;
		}
		printf("|\n");

	}
	
	print_split_line(num_cols);
}



token_list* get_where_for_select(token_list *cur, int &rc)
{
	t_list * where_cur = NULL;
	t_list* counter = cur;
	bool where = false;
	while ((counter->tok_value != EOC && counter->tok_value != K_WHERE) && !rc && cur != NULL)
	{		
		counter = counter->next;		
	}


	if (counter->tok_value == K_WHERE)
		where_cur = counter->next;

	return where_cur;
}

token_list* get_order_by(token_list *cur, int &rc)
{
	t_list * order_by_cur = NULL;
	t_list* counter = cur;
	bool where = false;
	while ((counter->tok_value != EOC && counter->tok_value != K_ORDER) && !rc && cur != NULL)
	{		
		counter = counter->next;
	}

	if (counter->tok_value == K_ORDER)
	{
		order_by_cur = counter->next;
		if (order_by_cur->tok_value != K_BY)
		{
			rc = INVALID_COLUMN_NAME;
			order_by_cur->tok_value = INVALID;
			printf("INVALID ORDER BY: %s\n", order_by_cur->tok_string);

			return NULL;
		}

		order_by_cur = order_by_cur->next;
		if (order_by_cur->tok_value != IDENT)
		{
			rc = INVALID_COLUMN_NAME;
			order_by_cur->tok_value = INVALID;
			printf("INVALID ORDER BY: %s\n", order_by_cur->tok_string);

			return NULL;
		}
	}

	return order_by_cur;
}

int get_ascending_flag(token_list *cur, int &rc)
{	
	int ascending_flag = 0; // 0: not sort, 1: asc, 2: desc
	if (cur == NULL)
		return 0;

	ascending_flag = 1;
	t_list* counter = cur;
	bool where = false;
	
	while ((counter->tok_value != EOC) && !rc && cur != NULL)
	{
		if (counter->tok_value == K_DESC)
			ascending_flag = 2;
		counter = counter->next;
	}

	return ascending_flag;
}

int sem_delete(token_list *t_list)
{
	token_list *cur = t_list;
	tpd_entry *tab_entry;
	cd_entry  *columns;
	int rc = 0;

	if (cur->tok_value != K_FROM)
	{
		printf("ERROR: invalid statement: %s", cur->tok_string);
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
		{
			//printf("hi1\n");
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
			return rc;
		}
		else
		{
			cur = cur->next;
			columns = get_columns(tab_entry);

			token_list *where_cur = get_where_for_select(cur, rc);

			int record_size = get_record_size(columns, tab_entry->num_columns);

			FILE* fp;
			char *filename = (char*)malloc(sizeof(tab_entry->table_name) + 4);

			strcpy(filename, tab_entry->table_name);
			strcat(filename, ".tab");
			if ((fp = fopen(filename, "r+")) == NULL)
			{
				printf("ERROR: unable to read table from file\n");
				return FILE_OPEN_ERROR;
			}

			int record_count = 0;
			fread(&record_count, sizeof(int), 1, fp);

			char *buffer = (char*)malloc(record_size * record_count * sizeof(char));
			fread(buffer, record_size, record_count, fp);

			fclose(fp);

			int remain_count = 0;
			for (int i = 0; i < record_count; i++)
			{
				// check if current record can be updated
				char *row = buffer + record_size * i;
				if (is_where_satisfied(row, tab_entry, columns, where_cur) == true)
					continue;
				
				remain_count++;				
			}

			if ((fp = fopen(filename, "wb")) == NULL)
			{
				printf("ERROR: unable to write table from file\n");
				return FILE_OPEN_ERROR;
			}

			fwrite(&remain_count, sizeof(int), 1, fp);

			for (int i = 0; i < record_count; i++)
			{
				// check if current record can be updated
				char *row = buffer + record_size * i;
				if (is_where_satisfied(row, tab_entry, columns, where_cur) == true)
					continue;

				fwrite(row, record_size, 1, fp);
			}
			fclose(fp);

			free(buffer);

			printf("%d rows deleted\n", record_count - remain_count);
		}

	}
	return rc;
}

char* get_buffer(char *filename, int &size)
{
	size = 0;
	FILE *fp = fopen(filename, "r");
	if (fp == NULL)
		return NULL;

	fseek(fp, 0L, SEEK_END);
	size = ftell(fp);

	char *buffer = (char *)malloc(size * sizeof(char));
	fseek(fp, 0L, SEEK_SET);
	fread(buffer, size, 1, fp);

	fclose(fp);

	return buffer;
}

int sem_backup(token_list *t_list)
{
	token_list *cur = t_list;
	int rc = 0;
	
	if (cur->tok_value != K_TO)
	{
		printf("ERROR: invalid statement: %s", cur->tok_string);
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		char table_name[100];
		int size = 0;
		char *buffer = get_buffer("dbfile.bin", size);
		
		char *filename = cur->tok_string;

		FILE *fp2 = fopen(filename, "rb");
		if (fp2 != NULL)
		{
			rc = INVALID_STATEMENT;
			fclose(fp2);
			return rc;
		}

		FILE *fp = fopen(filename, "wb");

		fwrite(buffer, size, 1, fp);
		free(buffer);

		char *pos = (char*)&(g_tpd_list->tpd_start);
		int num_tables = g_tpd_list->num_tables;
		for (int i = 0; i < num_tables; i++)
		{
			tpd_entry *tpd_start = (tpd_entry*)pos;
			strcpy(table_name, tpd_start->table_name);
			strcat(table_name, ".tab");

			buffer = get_buffer(table_name, size);
			fwrite(&size, sizeof(int), 1, fp);
			fwrite(buffer, size, sizeof(char), fp);
			free(buffer);

			pos += tpd_start->tpd_size;
		}

		fclose(fp);

		return rc;
	}

	return rc;
}

char* backup_log()
{
	FILE *fp1 = fopen("db.log", "r");
	if (fp1 == NULL)
		return NULL;

	FILE *fp = NULL;

	int i = 1;
	char* backup_log = (char*) calloc(100, 1);
	while (true)
	{
		sprintf(backup_log, "db.log%d", i);

		fp = fopen(backup_log, "r");
		if (fp != NULL)
		{
			fclose(fp);
			i++;
			continue;
		}
		break;
	}

	fp = fopen(backup_log, "w");
	
	char ch;
	ch = fgetc(fp1);
	while (ch != EOF)
	{
		fputc(ch, fp);
		ch = fgetc(fp1);
	}

	fclose(fp);
	fclose(fp1);

	return backup_log;
}

void prune_log(char *backup_log, char *image_name, bool rf_flag )
{
	FILE *fp = fopen(backup_log, "r");
	if (fp == NULL)
		return;

	FILE *fp1 = fopen("db.log", "w");

	char line[200];
	size_t len = 200;

	char check_backup_line[100] = "BACKUP ";
	sprintf(check_backup_line, "%s %s\n", "BACKUP", image_name);
	
	while (fgets(line, len, fp)) {
		fprintf(fp1, "%s", line);
		if (strcmp(line, check_backup_line) == 0)
			break;
	}

	if (rf_flag)
	{
		fprintf(fp1, "%s\n", "RF_START");
		while (fgets(line, len, fp)) {
			fprintf(fp1, "%s", line);			
		}
	}
	
	fclose(fp1);
	fclose(fp);
}

void prune_log1(char *backup_log,  bool to_flag, char *timelimit)
{
	FILE *fp = fopen(backup_log, "r");
	if (fp == NULL)
		return;

	FILE *fp1 = fopen("db.log", "w");

	char line[200];
	size_t len = 200;


	while (fgets(line, len, fp)) {
		if (strcmp(line, "RF_START\n") == 0)
			break;
		fprintf(fp1, "%s", line);		
	}

	if (to_flag)
	{
		char line1[200] = "";
		while (fgets(line, len, fp)) {
			strcpy(line1, line);
			line1[14] = NULL;
			if (strcmp(line1, timelimit) > 0)
				break;

			fprintf(fp1, "%s", line);
		}
	}

	fclose(fp1);
	fclose(fp);
}

int sem_restore(token_list *t_list)
{
	token_list *cur = t_list;
	int rc = 0;
	
	if (cur->tok_value != K_FROM)
	{
		printf("ERROR: invalid statement: %s", cur->tok_string);
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		bool rf_flag = true;
		if (cur->next != NULL && cur->next->next != NULL &&	// without RF
			strcmp(cur->next->tok_string, "WITHOUT") == 0 &&
			strcmp(cur->next->next->tok_string, "RF") == 0
			)
			rf_flag = false;
		else
			rf_flag = true;

		char table_name[100];
		int size = 0;
		
		char *filename = cur->tok_string;
		char *buffer = NULL;

		FILE *fp = fopen(filename, "rb");
		tpd_list *tpd = (tpd_list*) malloc(sizeof(tpd_list));

		fread(tpd, sizeof(tpd_list), 1, fp);
		int db_file_bin_size = tpd->list_size;
		
		free(tpd);

		fseek(fp, 0, SEEK_SET);
		tpd = (tpd_list*)calloc(db_file_bin_size, 1);
		fread(tpd, db_file_bin_size, 1, fp);

		if (rf_flag)
			tpd->db_flags = ROLLFORWARD_PENDING;
		else
			tpd->db_flags = NORMAL;

		FILE *fp1 = fopen("dbfile.bin", "wb");
		fwrite(tpd, db_file_bin_size, 1, fp1);
		fclose(fp1);

		
		char *pos = (char*)&(g_tpd_list->tpd_start);

		int num_tables = g_tpd_list->num_tables;

		for (int i = 0; i < num_tables; i++)
		{
			tpd_entry *tpd_start = (tpd_entry*)pos;

			fread(&size, sizeof(int), 1, fp);

			buffer = (char*)calloc(size, 1);
			
			strcpy(table_name, tpd_start->table_name);
			strcat(table_name, ".tab");

			fread(buffer, size, 1, fp);

			fp1 = fopen(table_name, "wb");

			fwrite(buffer, size, 1, fp1);

			fclose(fp1);

			free(buffer);

			pos += tpd_start->tpd_size;
		}

		
		fclose(fp);

		free(tpd);

		char* backup = backup_log();
		if (backup != NULL)
			prune_log(backup, filename, rf_flag);
		
		free(backup);

		return rc;
	}

	return rc;
}

int sem_rollforward(token_list *t_list)
{
	token_list *cur = t_list;
	int rc = 0;

	bool to_flag = true;
	char timelimit[30];
	sprintf(timelimit, "%s", "9999999999999");
	if (cur->next != NULL && cur->next->next != NULL &&	// without RF
		strcmp(cur->next->tok_string, "TO") == 0
		)
	{
		to_flag = true;
		sprintf(timelimit, "%s", cur->next->next->tok_string);
	}
	else
		to_flag = false;

	FILE *fp = fopen("db.log", "r");

	char line[200];
	size_t len = 200;

	char rc_line[100] = "RF_START\n";
	bool rf_start_flag = false;
	while (fgets(line, len, fp)) {
		if (strcmp(line, rc_line) == 0)
		{
			rf_start_flag = true;
			break;
		}
	}

	if (rf_start_flag == false)
		return -1;

	free(g_tpd_list);

	

	while (fgets(line, len, fp)) {
		line[strlen(line) - 1] = NULL;
		char* p = strchr(line, '"');
		char *command = p + 1;

		command[strlen(command) - 1] = NULL;

		line[14] = NULL;
		if (strcmp(line, timelimit) > 0)
			break;

		rc = initialize_tpd_list();

		if (rc)
		{
			printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
		}
		else
		{
			token_list *tok_list = NULL, *tok_ptr = NULL, *tmp_tok_ptr = NULL;

			rc = get_token(command, &tok_list);

			/* Test code */
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				printf("%16s \t%d \t %d\n", tok_ptr->tok_string, tok_ptr->tok_class,
					tok_ptr->tok_value);
				tok_ptr = tok_ptr->next;
			}

			if (!rc)
			{
				rc = do_semantic(tok_list, command);
			}

			if (rc)
			{
				tok_ptr = tok_list;
				while (tok_ptr != NULL)
				{
					if ((tok_ptr->tok_class == error) ||
						(tok_ptr->tok_value == INVALID))
					{
						printf("\nError in the string: %s\n", tok_ptr->tok_string);
						printf("rc=%d\n", rc);
						break;
					}
					tok_ptr = tok_ptr->next;
				}
			}

			/* Whether the token list is valid or not, we need to free the memory */
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				tmp_tok_ptr = tok_ptr->next;
				free(tok_ptr);
				tok_ptr = tmp_tok_ptr;
			}
		}

		free(g_tpd_list);
	}

	fclose(fp);

	char *backup = backup_log();
	prune_log1(backup, to_flag, timelimit);
	free(backup);

	// reset the db_flag = 0 
	fp = fopen("dbfile.bin", "rb+");
	tpd_list *tpd = (tpd_list*)calloc(1, sizeof(tpd_list));

	fread(tpd, sizeof(tpd_list), 1, fp);
	tpd->db_flags = NORMAL;
	fseek(fp, 0, SEEK_SET);
	fwrite(tpd, sizeof(tpd_list), 1, fp);
	fclose(fp);

	free(tpd);

	return rc;

}

