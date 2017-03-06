/*-------------------------------------------------------------------------
 *
 * pglogical_apply_spi.h
 * 		pglogical apply functions using SPI
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pglogical_apply_spi.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGLOGICAL_APPLY_SPI_H
#define PGLOGICAL_APPLY_SPI_H

#include "pglogical_relcache.h"
#include "pglogical_proto_native.h"

extern void pglogical_apply_spi_begin(void);
extern void pglogical_apply_spi_commit(void);

extern void pglogical_apply_spi_insert(PGLogicalRelation *rel,
									   PGLogicalTupleData *newtup);
extern void pglogical_apply_spi_update(PGLogicalRelation *rel,
									   PGLogicalTupleData *oldtup,
									   PGLogicalTupleData *newtup);
extern void pglogical_apply_spi_delete(PGLogicalRelation *rel,
									   PGLogicalTupleData *oldtup);

extern bool pglogical_apply_spi_can_mi(PGLogicalRelation *rel);
extern void pglogical_apply_spi_mi_add_tuple(PGLogicalRelation *rel,
											 PGLogicalTupleData *tup);
extern void pglogical_apply_spi_mi_finish(PGLogicalRelation *rel);

#endif /* PGLOGICAL_APPLY_SPI_H */
