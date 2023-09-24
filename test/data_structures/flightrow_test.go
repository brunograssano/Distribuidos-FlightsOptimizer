package data_structures

import (
	"DistribuidosTP1/data_structures"
	"testing"
)

func TestGetAsAllGetsOfNonExistentColumnShouldThrowError(t *testing.T) {
	dynMap := make(map[string]interface{})
	dynMap["test"] = 32
	row := data_structures.NewFlightRow(dynMap)
	_, err := row.GetAsInt("non_existent")
	if err == nil {
		t.Errorf("GetAsInt should have thrown error")
	}
	_, err = row.GetAsFloat("non_existent")
	if err == nil {
		t.Errorf("GetAsFloat should have thrown error")
	}
	_, err = row.GetAsString("non_existent")
	if err == nil {
		t.Errorf("GetAsString should have thrown error")
	}
	_, err = row.GetAsAny("non_existent")
	if err == nil {
		t.Errorf("GetAsAny should have thrown error")
	}
}

func TestGetAsIntAnIntColumn(t *testing.T) {
	dynMap := make(map[string]interface{})
	dynMap["test"] = 32
	row := data_structures.NewFlightRow(dynMap)
	val, err := row.GetAsInt("test")
	if val != 32 {
		t.Errorf("Value %v is not equal to 32", val)
	}
	if err != nil {
		t.Errorf("Thrown error: %v", err)
	}
}

func TestGetAsIntAFloat32Column(t *testing.T) {
	dynMap := make(map[string]interface{})
	dynMap["test"] = float32(32.0)
	row := data_structures.NewFlightRow(dynMap)
	val, err := row.GetAsInt("test")
	if val != 32 {
		t.Errorf("Value %v is not equal to 32", val)
	}
	if err != nil {
		t.Errorf("Thrown error: %v", err)
	}
}

func TestGetAsIntAFloat32TruncatesIt(t *testing.T) {
	dynMap := make(map[string]interface{})
	dynMap["test"] = float32(32.6)
	row := data_structures.NewFlightRow(dynMap)
	val, err := row.GetAsInt("test")
	if val != 32 {
		t.Errorf("Value %v is not equal to 32", val)
	}
	if err != nil {
		t.Errorf("Thrown error: %v", err)
	}
}

func TestGetAsIntAStringColumn(t *testing.T) {
	dynMap := make(map[string]interface{})
	dynMap["test"] = "32"
	row := data_structures.NewFlightRow(dynMap)
	val, err := row.GetAsInt("test")
	if val != 32 {
		t.Errorf("Value %v is not equal to 32", val)
	}
	if err != nil {
		t.Errorf("Thrown error: %v", err)
	}
}

func TestGetAsIntANonCasteableColumn(t *testing.T) {
	dynMap := make(map[string]interface{})
	dynMap["test"] = "Non Casteable String"
	row := data_structures.NewFlightRow(dynMap)
	val, err := row.GetAsInt("test")
	if val != 0 {
		t.Errorf("Value %v is not equal to 0 (Returned on error)", val)
	}
	if err == nil {
		t.Errorf("Did not throw error and should have thrown.")
	}
}

func TestGetAsFloatAnIntColumn(t *testing.T) {
	dynMap := make(map[string]interface{})
	dynMap["test"] = 32
	row := data_structures.NewFlightRow(dynMap)
	val, err := row.GetAsFloat("test")
	if val != 32.0 {
		t.Errorf("Value %v is not equal to 32.0", val)
	}
	if err != nil {
		t.Errorf("Thrown error: %v", err)
	}
}

func TestGetAsFloatAFloat32Column(t *testing.T) {
	dynMap := make(map[string]interface{})
	dynMap["test"] = float32(32.0)
	row := data_structures.NewFlightRow(dynMap)
	val, err := row.GetAsFloat("test")
	if val != 32.0 {
		t.Errorf("Value %v is not equal to 32.0", val)
	}
	if err != nil {
		t.Errorf("Thrown error: %v", err)
	}
}

func TestGetAsFloatAStringColumn(t *testing.T) {
	dynMap := make(map[string]interface{})
	dynMap["test"] = "32.0"
	row := data_structures.NewFlightRow(dynMap)
	val, err := row.GetAsFloat("test")
	if val != 32.0 {
		t.Errorf("Value %v is not equal to 32.0", val)
	}
	if err != nil {
		t.Errorf("Thrown error: %v", err)
	}
}

func TestGetAsFloatANonCasteableColumn(t *testing.T) {
	dynMap := make(map[string]interface{})
	dynMap["test"] = "Non Casteable String"
	row := data_structures.NewFlightRow(dynMap)
	val, err := row.GetAsInt("test")
	if val != 0.0 {
		t.Errorf("Value %v is not equal to 0 (Returned on error)", val)
	}
	if err == nil {
		t.Errorf("Did not throw error and should have thrown.")
	}
}

func TestGetAsStringAStringColumn(t *testing.T) {
	dynMap := make(map[string]interface{})
	dynMap["test"] = "stringval"
	row := data_structures.NewFlightRow(dynMap)
	val, err := row.GetAsString("test")
	if val != "stringval" {
		t.Errorf("Value %v is not equal to stringval", val)
	}
	if err != nil {
		t.Errorf("Thrown error: %v", err)
	}
}

func TestGetAsStringAnIntColumn(t *testing.T) {
	dynMap := make(map[string]interface{})
	dynMap["test"] = 32
	row := data_structures.NewFlightRow(dynMap)
	val, err := row.GetAsString("test")
	if val != "32" {
		t.Errorf("Value %v is not equal to 32", val)
	}
	if err != nil {
		t.Errorf("Thrown error: %v", err)
	}
}

func TestGetAsStringAFloatColumn(t *testing.T) {
	dynMap := make(map[string]interface{})
	dynMap["test"] = float32(32.01)
	row := data_structures.NewFlightRow(dynMap)
	val, err := row.GetAsString("test")
	if val != "32.01" {
		t.Errorf("Value %v is not equal to 32.01", val)
	}
	if err != nil {
		t.Errorf("Thrown error: %v", err)
	}
}
