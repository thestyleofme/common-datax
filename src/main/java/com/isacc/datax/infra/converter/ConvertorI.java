package com.isacc.datax.infra.converter;

/**
 * description
 *
 * @author isacc 2019/06/03 14:02
 */
public interface ConvertorI<E, D, T> {

    /**
     * dtoToEntity
     *
     * @param dto DTO
     * @return E
     * @author isacc 2019/6/3 14:03
     */
    default E dtoToEntity(T dto) {
        return null;
    }

    /**
     * entityToDto
     *
     * @param entity Entity
     * @return DTO
     * @author isacc 2019/6/3 14:03
     */
    default T entityToDto(E entity) {
        return null;
    }

    /**
     * doToEntity
     *
     * @param dataObject DO
     * @return E
     * @author isacc 2019/6/3 14:03
     */
    default E doToEntity(D dataObject) {
        return null;
    }

    /**
     * entityToDo
     *
     * @param entity Entity
     * @return Entity
     * @author isacc 2019/6/3 14:03
     */
    default D entityToDo(E entity) {
        return null;
    }

    /**
     * doToDto
     *
     * @param dataObject DO
     * @return DTO
     * @author isacc 2019/6/3 14:03
     */
    default T doToDto(D dataObject) {
        return null;
    }

    /**
     * dtoToDo
     *
     * @param dto DTO
     * @return DO
     * @author isacc 2019/6/3 14:03
     */
    default D dtoToDo(T dto) {
        return null;
    }

}
