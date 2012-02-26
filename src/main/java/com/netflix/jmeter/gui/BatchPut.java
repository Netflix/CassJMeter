package com.netflix.jmeter.gui;

import java.awt.GridBagConstraints;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.border.BevelBorder;

import org.apache.jmeter.testelement.TestElement;

import com.netflix.jmeter.sampler.AbstractSampler;
import com.netflix.jmeter.sampler.BatchPutSampler;

public class BatchPut extends AbstractGUI
{
    private static final long serialVersionUID = 3197090412869386190L;
    public static String LABEL = "Cassandra Batch Put";
    private JTextArea NAME_AND_VALUE;
    private JComboBox CSERIALIZER;
    private JComboBox VSERIALIZER;
    private JCheckBox IS_COUNTER;

    @Override
    public void configure(TestElement element)
    {
        super.configure(element);
        NAME_AND_VALUE.setText(element.getPropertyAsString(BatchPutSampler.NAME_AND_VALUE));
        CSERIALIZER.setSelectedItem(element.getPropertyAsString(BatchPutSampler.COLUMN_SERIALIZER_TYPE));
        VSERIALIZER.setSelectedItem(element.getPropertyAsString(BatchPutSampler.VALUE_SERIALIZER_TYPE));
        IS_COUNTER.setSelected(element.getPropertyAsBoolean(BatchPutSampler.IS_COUNTER));
    }

    public TestElement createTestElement()
    {
        BatchPutSampler sampler = new BatchPutSampler();
        modifyTestElement(sampler);
        sampler.setComment("test comment");
        return sampler;
    }

    public void modifyTestElement(TestElement sampler)
    {
        super.configureTestElement(sampler);
        if (sampler instanceof BatchPutSampler)
        {
            BatchPutSampler gSampler = (BatchPutSampler) sampler;
            gSampler.setCSerializerType((String) CSERIALIZER.getSelectedItem());
            gSampler.setVSerializerType((String) VSERIALIZER.getSelectedItem());
            gSampler.setNameValue(NAME_AND_VALUE.getText());
            gSampler.setCounter(IS_COUNTER.isSelected());
        }
    }

    public void initFields()
    {
        NAME_AND_VALUE.setText("${__Random(1,1000)}:${__Random(1,1000)}\n${__Random(1,1000)}:${__Random(1,1000)}");
        CSERIALIZER.setSelectedItem("Column Serializer");
        VSERIALIZER.setSelectedItem("Value Serializer");
        IS_COUNTER.setSelected(false);
    }

    public void init(JPanel mainPanel, GridBagConstraints labelConstraints, GridBagConstraints editConstraints)
    {
        addToPanel(mainPanel, labelConstraints, 0, 3, new JLabel("Column K/V(eg: Name:Value): ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 3, NAME_AND_VALUE = new JTextArea());
        NAME_AND_VALUE.setRows(10);
        NAME_AND_VALUE.setBorder(new BevelBorder(BevelBorder.LOWERED));
        addToPanel(mainPanel, labelConstraints, 0, 4, new JLabel("Column Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 4, CSERIALIZER = new JComboBox(AbstractSampler.getSerializerNames().toArray()));
        addToPanel(mainPanel, labelConstraints, 0, 5, new JLabel("Value Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 5, VSERIALIZER = new JComboBox(AbstractSampler.getSerializerNames().toArray()));
        addToPanel(mainPanel, labelConstraints, 0, 6, new JLabel("Counter: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 6, IS_COUNTER = new JCheckBox());
    }

    @Override
    public String getLable()
    {
        return LABEL;
    }
}
