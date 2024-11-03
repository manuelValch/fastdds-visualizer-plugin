// Copyright 2022 Proyectos y Sistemas de Mantenimiento SL (eProsima).
//
// This file is part of eProsima Fast DDS Visualizer Plugin.
//
// eProsima Fast DDS Visualizer Plugin is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// eProsima Fast DDS Visualizer Plugin is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with eProsima Fast DDS Visualizer Plugin. If not, see <https://www.gnu.org/licenses/>.

/**
 * @file ReaderHandler.hpp
 */

#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastrtps/xmlparser/XMLProfileManager.h>
#include <fastrtps/types/DynamicDataHelper.hpp>
#include <fastrtps/types/DynamicDataFactory.h>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>

#include "ReaderHandler.hpp"
#include "utils/utils.hpp"
#include "utils/dynamic_types_utils.hpp"

namespace eprosima {
namespace plotjuggler {
namespace fastdds {


////////////////////////////////////////////////////
// CREATION & DESTRUCTION
////////////////////////////////////////////////////

ReaderHandler::ReaderHandler(
        eprosima::fastdds::dds::Topic* topic,
        eprosima::fastdds::dds::DataReader* datareader,
        eprosima::fastrtps::types::DynamicType_ptr type,
        FastDdsListener* listener,
        const DataTypeConfiguration& data_type_configuration,
        const bool& is_Keyed)
    : topic_(topic)
    , reader_(datareader)
    , type_(type)
    , listener_(listener)
    , stop_(false)
    , is_keyed_(is_Keyed)
{
    // Create data so it is not required to create it each time and avoid reallocation if possible
    data_ = eprosima::fastrtps::types::DynamicDataFactory::get_instance()->create_data(type_);

    auto refDesc = type_->get_descriptor();

    // Save data type configuration for latter use
    data_type_configurations_[topic_name()] = &data_type_configuration;

    // if topic is not keyed then structure of data can be created right away otherwise we need to wait for data to arrive
    if (false == is_keyed_)
    {
        createTypeInstrospection(topic_name());
    }


    // Set this object as this reader's listener
    reader_->set_listener(this);
}

ReaderHandler::~ReaderHandler()
{
    // Stop the reader
    stop();

    // Delete created data
    eprosima::fastrtps::types::DynamicDataFactory::get_instance()->delete_data(data_);
}

////////////////////////////////////////////////////
// INTERACTION METHODS
////////////////////////////////////////////////////

void ReaderHandler::stop()
{
    // Stop the reader
    stop_ = true;
    reader_->set_listener(nullptr);
}

////////////////////////////////////////////////////
// LISTENER METHODS [ DATAREADER ]
////////////////////////////////////////////////////

void ReaderHandler::on_data_available(
        eprosima::fastdds::dds::DataReader* reader)
{
    eprosima::fastdds::dds::SampleInfo info;
    eprosima::fastrtps::types::ReturnCode_t read_ret =
            eprosima::fastrtps::types::ReturnCode_t::RETCODE_OK;

    // Read Data from reader while there is data available and not should stop
    while (!stop_ && read_ret == eprosima::fastrtps::types::ReturnCode_t::RETCODE_OK)
    {
        // Read next data
        read_ret = reader->take_next_sample(data_, &info);

        // If data has been read
        if (read_ret == eprosima::fastrtps::types::ReturnCode_t::RETCODE_OK &&
                info.instance_state == eprosima::fastdds::dds::InstanceStateKind::ALIVE_INSTANCE_STATE)
        {
            if (true == is_keyed_)
            {
                on_data_keyed(reader->get_topicdescription()->get_name(), info);
            }
            else
            {
                process_received_data(info);
            }
        }
    }
}

void ReaderHandler::on_data_keyed(const std::string& topic_name,
                                    const eprosima::fastdds::dds::SampleInfo& sample_info)
{
    // construct the key
    std::map<eprosima::fastrtps::types::MemberId, eprosima::fastrtps::types::DynamicTypeMember*> members;
    type_->get_all_members(members);
    std::string key="/";
    for (auto const& [memberId, member] : members)
    {
        // If member is a key then build up a new type instrospection for Plojuggler
        if (member->key_annotation())
        {
            std::string key_value;
            eprosima::fastrtps::types::MemberDescriptor member_descriptor = member->get_descriptor();
            key +=  member_descriptor.get_name() + ": ";
            // check return might have to change this for the return string or double
            const auto member_kind = member_descriptor.get_kind();
            if (true == utils::is_kind_string(member_kind))
            {
                key += utils::get_string_type_from_data(data_, memberId, member_kind) + ", ";
            }
            else if (true == utils::is_kind_numeric(member_kind))
            {
                key += std::to_string(utils::get_numeric_type_from_data(data_, memberId, member_kind)) + ", ";
            }
            else
            {
                WARNING("Member type unknown with name: " << member_descriptor.get_name());
            }
        }
    }

    // Once key is fully formed then we can check if we need a new type configuration
    if (data_type_configurations_.find(data_->get_name() + key) == data_type_configurations_.end())
    {
        data_type_configurations_[data_->get_name() + key] = data_type_configurations_[data_->get_name()];
        createTypeInstrospection(topic_name, key);
        DEBUG("New topic instance received with key = " << key);
    }

    // Now we can process the data from members that are not keys
    for (auto const& [memberId, member] : members)
    {
        if (false == member->key_annotation())
        {
            process_received_data(sample_info);
        }
        else
        {
            // Do nothing key is already processed
        }
    }
}

void ReaderHandler::process_received_data(const eprosima::fastdds::dds::SampleInfo& sample_info)
{
    // Get timestamp
    double timestamp = utils::get_timestamp_seconds_numeric_value(sample_info.reception_timestamp);

    // Get data in already created structures
    utils::get_introspection_numeric_data(
        numeric_data_info_,
        data_,
        numeric_data_);

    utils::get_introspection_string_data(
        string_data_info_,
        data_,
        string_data_);

    // Get value maps from data and send callback if there are data
    if (!numeric_data_.empty())
    {
        listener_->on_double_data_read(
            numeric_data_,
            timestamp);
    }

    // Same for strings
    if (!string_data_.empty())
    {
        listener_->on_string_data_read(
            string_data_,
            timestamp);
    }
}

////////////////////////////////////////////////////
// VALUES METHODS
////////////////////////////////////////////////////

const std::string& ReaderHandler::topic_name() const
{
    return topic_->get_name();
}

const std::string& ReaderHandler::type_name() const
{
    return topic_->get_type_name();
}


////////////////////////////////////////////////////
// AUXILIAR METHODS
////////////////////////////////////////////////////

void ReaderHandler::createTypeInstrospection(const std::string& topic_name, const std::string& key)
{
    std::string topic_key_names = topic_name + key;
    if (data_type_configurations_.find(topic_key_names) == data_type_configurations_.end())
    {
        // Create the static structures to store the data introspection information AND the data itself
        utils::get_introspection_type_names(
            topic_key_names,
            type_,
            *data_type_configurations_[topic_key_names],
            numeric_data_info_,
            string_data_info_);

        // Create the data structures so they are not copied in the future
        for (const auto& info : numeric_data_info_)
        {
            numeric_data_.push_back({ std::get<0>(info), 0});
        }
        for (const auto& info : string_data_info_)
        {
            string_data_.push_back({ std::get<0>(info), "-"});
        }

        DEBUG("Reader created in topic: " << topic_key_names << " with types: ");
        for (const auto& info : numeric_data_info_)
        {
            DEBUG("\tNumeric: " << std::get<0>(info));
        }
        for (const auto& info : string_data_info_)
        {
            DEBUG("\tString: " << std::get<0>(info));
        }
    }
    else
    {
        WARNING("\tTrying to create data structure for a non selected topic");
    }
}

////////////////////////////////////////////////////
// AUXILIAR STATIC METHODS
////////////////////////////////////////////////////

eprosima::fastdds::dds::StatusMask ReaderHandler::default_listener_mask_()
{
    // Start from all bits set to 0
    eprosima::fastdds::dds::StatusMask mask = eprosima::fastdds::dds::StatusMask::none();

    // Only listen this callback (and the DomainParticipantListener ones)
    mask << eprosima::fastdds::dds::StatusMask::data_available();

    return mask;
}

std::vector<std::string> ReaderHandler::numeric_data_series_names() const
{
    return utils::get_introspection_type_names(numeric_data_info_);
}

std::vector<std::string> ReaderHandler::string_data_series_names() const
{
    return utils::get_introspection_type_names(string_data_info_);
}

} /* namespace fastdds */
} /* namespace plotjuggler */
} /* namespace eprosima */
