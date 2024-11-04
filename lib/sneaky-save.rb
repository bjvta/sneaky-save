#--
# Copyright (c) 2011 {PartyEarth LLC}[http://partyearth.com]
# mailto:kgoslar@partyearth.com
#++
module SneakySave
  # Saves the record without running callbacks/validations.
  # Returns true if the record is changed.
  # @note - Does not reload updated record by default.
  #       - Does not save associated collections.
  #       - Saves only belongs_to relations.
  #
  # @return [false, true]
  def sneaky_save
    begin
      sneaky_create_or_update
    rescue ActiveRecord::StatementInvalid
      false
    end
  end

  # Saves record without running callbacks/validations.
  # @see ActiveRecord::Base#sneaky_save
  # @return [true] if save was successful.
  # @raise [ActiveRecord::StatementInvalid] if saving failed.
  def sneaky_save!
    sneaky_create_or_update
  end

  protected

  def sneaky_create_or_update
    new_record? ? sneaky_create : sneaky_update
  end

  # Performs INSERT query without running any callbacks
  # @return [false, true]
  def sneaky_create
    prefetch_pk_allowed = sneaky_connection.prefetch_primary_key?(self.class.table_name)

    if id.nil? && prefetch_pk_allowed
      self.id = sneaky_connection.next_sequence_value(self.class.sequence_name)
    end

    # attributes_values = sneaky_attributes_values
    attributes_values = get_attribute_values

    # puts attributes_values

    # Remove the id field for databases like Postgres
    # which fail with id passed as NULL
    if id.nil? && !prefetch_pk_allowed
      attributes_values.reject! { |key, _| key.try(:name) == 'id' }
    end

    if attributes_values.empty?
      new_id = self.class.unscoped.insert(sneaky_connection.empty_insert_statement_value)
    else
      new_id = insert_record(attributes_values)
    end

    @new_record = false
    !!(self.id ||= new_id)
  end

  def insert_record(attributes_values)
    # Get the table name
    table_name = self.class.table_name

    # Format the columns and values for SQL
    # debugger
    columns = attributes_values.keys.map { |key| self.class.connection.quote_column_name(key) }.join(', ')
    values = attributes_values.values.map { |value| self.class.connection.quote(value) }.join(', ')

    # Perform the insert operation
    new_id = self.class.connection.insert("INSERT INTO #{self.class.connection.quote_table_name(table_name)} (#{columns}) VALUES (#{values})")

    # Return the newly inserted id
    new_id
  end

  def get_attribute_values
    # Get the raw attribute values without type casting
    attributes_values = self.attributes_before_type_cast

    # Handle the case where id is nil and prefetch_pk is not allowed
    if self.id.nil? && !self.class.connection.prefetch_primary_key?(self.class.table_name)
      attributes_values.reject! { |key, _| key == 'id' }
    end

    attributes_values
  end

  # Performs update query without running callbacks
  # @return [false, true]
  def sneaky_update
    ActiveRecord::Base.use_yaml_unsafe_load = true
    return true if changes.empty?

    pk = self.class.primary_key
    original_id = changed_attributes.key?(pk) ? changes[pk].first : send(pk)

    changed_attributes = sneaky_update_fields

    # Serialize values for rails3 before updating
    unless sneaky_new_rails?
      serialized_fields = self.class.serialized_attributes.keys & changed_attributes.keys
      serialized_fields.each do |field|
        changed_attributes[field] = @attributes[field].serialized_value
      end
    end

    !self.class.unscoped.where(pk => original_id).
      update_all(changed_attributes).zero?
  end

  # def sneaky_attributes_values
  #   if sneaky_new_rails?
  #     send :arel_attributes_with_values_for_create, attribute_names
  #     attributes_hash = attributes.except('id')

  #     # Return the attributes as a struct using OpenStruct
  #     OpenStruct.new(attributes_hash)
  #   else
  #     send :arel_attributes_values
  #   end
  # end

  def sneaky_update_fields
    changes.keys.each_with_object({}) do |field, result|
      result[field] = read_attribute(field)
    end
  end

  def sneaky_connection
    if sneaky_new_rails?
      self.class.connection
    else
      connection
    end
  end

  def sneaky_new_rails?
    ActiveRecord::VERSION::STRING.to_i > 3
  end
end

ActiveRecord::Base.send :include, SneakySave
